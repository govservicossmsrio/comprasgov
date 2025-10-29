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
MAX_CONCURRENT_REQUESTS = 3
TIMEOUT = 90
MAX_RETRIES = 5 # Máximo de tentativas para o rate limit

# --- Funções Auxiliares ---
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

# --- Funções de Banco de Dados ---
def get_db_connection():
    try: return psycopg2.connect(CONN_STRING)
    except psycopg2.OperationalError as e: logging.error(f"Falha ao criar conexão inicial: {e}"); return None

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

# =================================================================
# CORREÇÃO 1: Função agora recebe a conexão, não a string.
# =================================================================
def sync_precos_catalogo(conn: psycopg2.extensions.connection, codigo_item: str, tipo_item: str, precos_api: List[Dict[str, Any]]) -> Tuple[int, int]:
    if not precos_api: return 0, 0
    novos, atualizados = 0, 0
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
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
            novos = len(dados_insert)

        if precos_para_atualizar:
            dados_update = [(p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'), p.get('quantidade_item'), p.get('valor_unitario_homologado'), p.get('valor_total_homologado'), p.get('nome_vencedor'), p.get('data_resultado'), codigo_item, p.get('numero_compra'), p.get('cnpj_vencedor')) for p in precos_para_atualizar]
            psycopg2.extras.execute_batch(cur, "UPDATE precos_catalogo SET descricao_item = %s, unidade_medida = %s, quantidade_total = %s, valor_unitario = %s, valor_total = %s, nome_fornecedor = %s, data_compra = %s, data_atualizacao = CURRENT_TIMESTAMP WHERE codigo_item_catalogo = %s AND id_compra = %s AND ni_fornecedor = %s;", dados_update)
            atualizados = len(dados_update)
            
    return novos, atualizados

# =================================================================
# CORREÇÃO 2: Lógica de retentativa com backoff exponencial.
# =================================================================
async def fetch_precos_item(session: aiohttp.ClientSession, codigo_item: str, tipo_item: str, ultima_data: Optional[datetime]) -> Optional[List[Dict[str, Any]]]:
    tipo_normalizado = tipo_item.strip().lower()
    base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV" if 'material' in tipo_normalizado else "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV"
    
    params = {'codigoItemCatalogo': codigo_item, 'pagina': 1, 'tamanhoPagina': 500}
    
    if ultima_data:
        data_inicio_busca = ultima_data + timedelta(days=1)
        params['dataInicio'] = data_inicio_busca.strftime('%d/%m/%Y')
        logging.info(f"Buscando item {codigo_item} (a partir de {params['dataInicio']})...")
    else:
        logging.info(f"Buscando item {codigo_item} (histórico completo)...")

    all_dfs, current_page = [], 1
    retries = 0
    while True:
        params['pagina'] = current_page
        try:
            async with session.get(base_url, params=params, headers={'accept': 'text/csv'}, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
                if response.status == 429:
                    if retries < MAX_RETRIES:
                        wait_time = 5 * (2 ** retries) # 5s, 10s, 20s...
                        logging.warning(f"Item {codigo_item}: Rate limit (429) na página {current_page}. Tentativa {retries+1}/{MAX_RETRIES}. Aguardando {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                        continue # Tenta a mesma página novamente
                    else:
                        logging.error(f"Item {codigo_item}: Rate limit excedido após {MAX_RETRIES} tentativas. Desistindo deste item.")
                        return None # Retorna None para indicar falha

                if response.status != 200: break
                
                retries = 0 # Reseta as tentativas após um sucesso
                content_bytes = await response.read();
                if not content_bytes: break
                cleaned_csv_text = _decode_and_clean_csv(content_bytes)
                if len(cleaned_csv_text.strip().splitlines()) < 2: break
                df_page = pd.read_csv(io.StringIO(cleaned_csv_text), sep=';', on_bad_lines='warn', engine='python', dtype=str)
                if df_page.empty: break
                all_dfs.append(df_page)
                current_page += 1
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Erro ao processar página {current_page} para item {codigo_item}: {e}", exc_info=False); break
    
    if not all_dfs: return []
    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df.columns = [normalizar_nome_coluna(col) for col in full_df.columns]
    full_df = full_df.where(pd.notna(full_df), None)
    precos = full_df.to_dict('records')
    logging.info(f"SUCESSO: {len(precos)} novos preços coletados para o item {codigo_item}.")
    return precos

# --- Orquestração Principal ---
async def process_itens_concorrently(conn_string: str, itens_a_processar: Dict[str, Dict]) -> Dict[str, int]:
    stats = {'total_itens': len(itens_a_processar), 'itens_processados': 0, 'precos_novos': 0, 'precos_atualizados': 0, 'erros': 0}
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_and_sync_item(codigo: str, detalhes: Dict) -> None:
        async with semaphore:
            conn = None
            try:
                precos = None
                async with aiohttp.ClientSession() as session:
                    precos = await fetch_precos_item(session, codigo, detalhes['tipo'], detalhes['ultima_data'])
                
                if precos is not None:
                    # Abre a conexão apenas se tiver dados para salvar
                    conn = psycopg2.connect(conn_string)
                    novos, atualizados = sync_precos_catalogo(conn, codigo, detalhes['tipo'], precos)
                    conn.commit() # Commit explícito por item!
                    stats['precos_novos'] += novos
                    stats['precos_atualizados'] += atualizados
                    if novos > 0 or atualizados > 0: 
                        logging.info(f"Item {codigo}: Sincronizado com sucesso ({novos} novos, {atualizados} atualizados).")
                else:
                    stats['erros'] += 1
                stats['itens_processados'] += 1
            except Exception as e:
                logging.error(f"Erro CRÍTICO no fluxo do item {codigo}: {e}", exc_info=True)
                stats['erros'] += 1
                if conn:
                    conn.rollback() # Desfaz a transação em caso de erro
            finally:
                if conn:
                    conn.close() # Garante que a conexão seja sempre fechada

    tasks = [fetch_and_sync_item(codigo, detalhes) for codigo, detalhes in itens_a_processar.items()]
    await asyncio.gather(*tasks)
    return stats

async def main():
    conn = get_db_connection()
    if not conn: logging.error("Abortando."); return
    try:
        itens_para_processar = get_itens_para_processar(conn)
        if not itens_para_processar: logging.info("Nenhum item para processar."); return
    finally: conn.close()
    
    stats = await process_itens_concorrently(CONN_STRING, itens_para_processar)
    
    logging.info("=" * 60 + "\nRESUMO FINAL DA SINCRONIZAÇÃO\n" + "=" * 60)
    for key, value in stats.items(): logging.info(f"{key.replace('_', ' ').title()}: {value}")
    logging.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())