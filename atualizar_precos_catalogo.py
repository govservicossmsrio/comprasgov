import os
import asyncio
import aiohttp
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
import io
import re
from charset_normalizer import from_bytes
from ftfy import fix_text

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
# Itens a serem buscados em paralelo dentro de cada lote
MAX_CONCURRENT_REQUESTS = 3
# Quantos itens processar antes de fazer uma gravação no banco
LOTE_SIZE = 10
TIMEOUT = 90
MAX_RETRIES = 5 # Máximo de tentativas para o rate limit

# --- Funções Auxiliares e de Banco (Síncronas) ---

def normalizar_nome_coluna(nome: str) -> str:
    if not isinstance(nome, str): return ''
    s = nome.strip(); s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s); s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

def _decode_and_clean_csv(raw_bytes: bytes) -> str:
    try:
        probe = from_bytes(raw_bytes).best()
        decoded_text = str(probe) if probe else raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        decoded_text = raw_bytes.decode("latin-1", errors="replace")
    
    fixed_text_content = fix_text(decoded_text)
    lines = fixed_text_content.strip().splitlines()
    if lines and "totalRegistros" in lines[-1]: lines.pop()
    return "\n".join(lines)

def get_db_connection():
    try:
        return psycopg2.connect(CONN_STRING)
    except psycopg2.OperationalError as e:
        logging.error(f"Falha ao criar conexão inicial: {e}")
        return None

def get_itens_para_processar(conn) -> Dict[str, Dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT DISTINCT codigo, TRIM(LOWER(tipo)) as tipo FROM itens_catalogo WHERE codigo IS NOT NULL AND tipo IS NOT NULL;")
        itens_base = cur.fetchall()
        if not itens_base: return {}
        cur.execute("SELECT codigo_item_catalogo, MAX(data_compra) as ultima_data FROM precos_catalogo GROUP BY codigo_item_catalogo;")
        ultimas_datas = {row['codigo_item_catalogo']: row['ultima_data'] for row in cur}
        itens_para_processar = {
            str(item['codigo']): {
                'tipo': item['tipo'],
                'ultima_data': ultimas_datas.get(str(item['codigo']))
            } for item in itens_base
        }
        logging.info(f"Encontrados {len(itens_para_processar)} itens únicos para verificar.")
        return itens_para_processar

def sync_lote_precos_catalogo(conn_string: str, resultados_lote: List[Dict]) -> Tuple[int, int, int]:
    """
    Função SÍNCRONA que recebe um lote de resultados e salva tudo em uma única transação.
    """
    total_novos, total_atualizados, total_erros_db = 0, 0, 0
    conn = None
    try:
        conn = psycopg2.connect(conn_string)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for resultado in resultados_lote:
                codigo_item = resultado['codigo']
                tipo_item = resultado['tipo']
                precos_api = resultado['precos']

                if not precos_api:
                    continue

                cur.execute("SELECT * FROM precos_catalogo WHERE codigo_item_catalogo = %s::VARCHAR", (codigo_item,))
                precos_db_map = {f"{row['id_compra']}-{row['ni_fornecedor']}": row for row in cur}
                
                precos_para_inserir, precos_para_atualizar = [], []
                for p in precos_api:
                    id_compra_api, ni_fornecedor_api = p.get('numero_compra', ''), p.get('cnpj_vencedor', '')
                    if not id_compra_api or not ni_fornecedor_api: continue
                    chave_api = f"{id_compra_api}-{ni_fornecedor_api}"
                    try: valor_unitario_api = float(p.get('valor_unitario_homologado', 0))
                    except (ValueError, TypeError): continue
                    
                    preco_existente = precos_db_map.get(chave_api)
                    if not preco_existente:
                        precos_para_inserir.append(p)
                    else:
                        if not psycopg2.extensions.Float(valor_unitario_api).isclose(preco_existente['valor_unitario']):
                            precos_para_atualizar.append(p)
                
                if precos_para_inserir:
                    dados_insert = [(codigo_item, tipo_item, p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'), p.get('quantidade_item'), p.get('valor_unitario_homologado'), p.get('valor_total_homologado'), p.get('cnpj_vencedor'), p.get('nome_vencedor'), p.get('numero_compra'), p.get('data_resultado'), datetime.now()) for p in precos_para_inserir]
                    psycopg2.extras.execute_values(cur, "INSERT INTO precos_catalogo (codigo_item_catalogo, tipo_item, descricao_item, unidade_medida, quantidade_total, valor_unitario, valor_total, ni_fornecedor, nome_fornecedor, id_compra, data_compra, data_atualizacao) VALUES %s", dados_insert)
                    total_novos += len(dados_insert)

                if precos_para_atualizar:
                    dados_update = [(p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'), p.get('quantidade_item'), p.get('valor_unitario_homologado'), p.get('valor_total_homologado'), p.get('nome_vencedor'), p.get('data_resultado'), codigo_item, p.get('numero_compra'), p.get('cnpj_vencedor')) for p in precos_para_atualizar]
                    psycopg2.extras.execute_batch(cur, "UPDATE precos_catalogo SET descricao_item = %s, unidade_medida = %s, quantidade_total = %s, valor_unitario = %s, valor_total = %s, nome_fornecedor = %s, data_compra = %s, data_atualizacao = CURRENT_TIMESTAMP WHERE codigo_item_catalogo = %s AND id_compra = %s AND ni_fornecedor = %s;", dados_update)
                    total_atualizados += len(dados_update)

        conn.commit()
        logging.info(f"Lote salvo com sucesso no banco: {total_novos} novos, {total_atualizados} atualizados.")
    except Exception as e:
        logging.error(f"Erro CRÍTICO ao salvar lote no banco: {e}", exc_info=True)
        total_erros_db = len(resultados_lote)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            
    return total_novos, total_atualizados, total_erros_db

# --- Funções de Coleta (Assíncronas) ---

async def fetch_precos_item(session: aiohttp.ClientSession, codigo_item: str, tipo_item: str, ultima_data: Optional[datetime]) -> Dict:
    """
    Busca dados de um item e retorna um dicionário estruturado com o resultado.
    """
    tipo_normalizado = tipo_item.strip().lower()
    base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV" if 'material' in tipo_normalizado else "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV"
    params = {'codigoItemCatalogo': codigo_item, 'pagina': 1, 'tamanhoPagina': 500}
    
    if ultima_data:
        data_inicio_busca = ultima_data + timedelta(days=1)
        params['dataInicio'] = data_inicio_busca.strftime('%d/%m/%Y')
        logging.info(f"Buscando item {codigo_item} (a partir de {params['dataInicio']})...")
    else:
        logging.info(f"Buscando item {codigo_item} (histórico completo)...")

    all_dfs, current_page, retries = [], 1, 0
    sucesso_coleta = True
    
    while True:
        params['pagina'] = current_page
        try:
            async with session.get(base_url, params=params, headers={'accept': 'text/csv'}, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
                if response.status == 429:
                    if retries < MAX_RETRIES:
                        wait_time = 5 * (2 ** retries)
                        logging.warning(f"Item {codigo_item}: Rate limit (429) na página {current_page}. Tentativa {retries+1}/{MAX_RETRIES}. Aguardando {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        logging.error(f"Item {codigo_item}: Rate limit excedido após {MAX_RETRIES} tentativas. Desistindo deste item.")
                        sucesso_coleta = False
                        break
                
                if response.status != 200:
                    break
                
                retries = 0
                content_bytes = await response.read()
                if not content_bytes: break
                cleaned_csv_text = _decode_and_clean_csv(content_bytes)
                if len(cleaned_csv_text.strip().splitlines()) < 2: break
                df_page = pd.read_csv(io.StringIO(cleaned_csv_text), sep=';', on_bad_lines='warn', engine='python', dtype=str)
                if df_page.empty: break
                all_dfs.append(df_page)
                current_page += 1
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Erro de rede/processamento para item {codigo_item}: {e}", exc_info=False)
            sucesso_coleta = False
            break
    
    precos_coletados = []
    if sucesso_coleta and all_dfs:
        try:
            full_df = pd.concat(all_dfs, ignore_index=True)
            full_df.columns = [normalizar_nome_coluna(col) for col in full_df.columns]
            full_df = full_df.where(pd.notna(full_df), None)
            precos_coletados = full_df.to_dict('records')
            logging.info(f"SUCESSO: {len(precos_coletados)} preços coletados para o item {codigo_item}.")
        except Exception as e:
            logging.error(f"Erro ao concatenar/processar DataFrame para o item {codigo_item}: {e}")
            sucesso_coleta = False

    return {
        'codigo': codigo_item,
        'tipo': tipo_item,
        'precos': precos_coletados,
        'sucesso': sucesso_coleta
    }

# --- Orquestração Principal (Nova Arquitetura de Lotes) ---

async def main():
    conn = get_db_connection()
    if not conn:
        logging.error("Abortando: não foi possível conectar ao banco de dados.")
        return
    
    try:
        itens_para_processar = get_itens_para_processar(conn)
    finally:
        conn.close()

    if not itens_para_processar:
        logging.info("Nenhum item para processar.")
        return

    stats = {'total_itens': len(itens_para_processar), 'itens_processados': 0, 'precos_novos': 0, 'precos_atualizados': 0, 'erros': 0}
    itens_lista = list(itens_para_processar.items())

    for i in range(0, len(itens_lista), LOTE_SIZE):
        lote_atual = itens_lista[i:i + LOTE_SIZE]
        logging.info(f"\n--- Processando Lote {i//LOTE_SIZE + 1}/{(len(itens_lista) + LOTE_SIZE - 1)//LOTE_SIZE} (Itens {i+1} a {i+len(lote_atual)}) ---\n")

        # PASSO 1: Coleta assíncrona dos dados do lote
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_precos_item(session, codigo, detalhes['tipo'], detalhes['ultima_data']) for codigo, detalhes in lote_atual]
            resultados_coleta = await asyncio.gather(*tasks)

        # PASSO 2: Sincronização síncrona do lote coletado
        resultados_validos = [res for res in resultados_coleta if res and res['sucesso']]
        
        if resultados_validos:
            novos, atualizados, erros_db = sync_lote_precos_catalogo(CONN_STRING, resultados_validos)
            stats['precos_novos'] += novos
            stats['precos_atualizados'] += atualizados
            stats['erros'] += erros_db
        
        stats['itens_processados'] += len(lote_atual)
        stats['erros'] += len(lote_atual) - len(resultados_validos)

    logging.info("=" * 60 + "\nRESUMO FINAL DA SINCRONIZAÇÃO\n" + "=" * 60)
    for key, value in stats.items():
        logging.info(f"{key.replace('_', ' ').title()}: {value}")
    logging.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())