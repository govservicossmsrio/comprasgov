import os
import asyncio
import aiohttp
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
import io
import re

# --- Configuração Inicial ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
MAX_CONCURRENT_REQUESTS = 10
TIMEOUT = 90

# --- Funções Auxiliares ---
def normalizar_nome_coluna(nome: str) -> str:
    if not isinstance(nome, str): return ''
    s = nome.strip(); s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s); s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

# --- Funções de Banco de Dados ---
def get_db_connection():
    try: return psycopg2.connect(CONN_STRING)
    except psycopg2.OperationalError as e: logging.error(f"Falha ao criar conexão inicial: {e}"); return None

def get_itens_catalogo_from_db(conn) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT codigo, TRIM(LOWER(tipo)) as tipo FROM itens_catalogo WHERE codigo IS NOT NULL AND tipo IS NOT NULL ORDER BY codigo;")
        itens = cur.fetchall()
        logging.info(f"Encontrados {len(itens)} itens únicos no banco para processar.")
        return itens

# =================================================================
# A ALTERAÇÃO CRÍTICA ESTÁ AQUI
# =================================================================
def sync_precos_catalogo(conn_string: str, codigo_item: str, tipo_item: str, precos_api: List[Dict[str, Any]]) -> Tuple[int, int]:
    if not precos_api: return 0, 0
    novos, atualizados = 0, 0
    
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # 1. Buscar preços existentes
            precos_db_map = {}
            cur.execute("SELECT * FROM precos_catalogo WHERE codigo_item_catalogo = %s", (codigo_item,))
            for row in cur:
                chave = f"{row['id_compra']}-{row['ni_fornecedor']}"
                precos_db_map[chave] = row

            # 2. Separar registros
            precos_para_inserir, precos_para_atualizar = [], []
            for p in precos_api:
                id_compra_api = str(p.get('numero_compra', ''))
                ni_fornecedor_api = str(p.get('cnpj_vencedor', ''))
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
            
            # 3. Executar Lote de Inserção (garantindo que os dados são strings)
            if precos_para_inserir:
                dados_insert = [(
                    codigo_item, tipo_item, p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'),
                    p.get('quantidade_item'), p.get('valor_unitario_homologado'), p.get('valor_total_homologado'),
                    str(p.get('cnpj_vencedor')), str(p.get('nome_vencedor')), str(p.get('numero_compra')),
                    p.get('data_resultado'), datetime.now()
                ) for p in precos_para_inserir]
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO precos_catalogo (codigo_item_catalogo, tipo_item, descricao_item, unidade_medida, quantidade_total, valor_unitario, valor_total, ni_fornecedor, nome_fornecedor, id_compra, data_compra, data_atualizacao) VALUES %s
                """, dados_insert)
                novos = len(dados_insert)

            # 4. Executar Lote de Atualização com CAST explícito no SQL
            if precos_para_atualizar:
                dados_update = [(
                    p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'), p.get('quantidade_item'), p.get('valor_unitario_homologado'),
                    p.get('valor_total_homologado'), str(p.get('nome_vencedor')), p.get('data_resultado'),
                    codigo_item, str(p.get('numero_compra')), str(p.get('cnpj_vencedor'))
                ) for p in precos_para_atualizar]
                psycopg2.extras.execute_batch(cur, """
                    UPDATE precos_catalogo SET
                        descricao_item = %s, unidade_medida = %s, quantidade_total = %s,
                        valor_unitario = %s, valor_total = %s, nome_fornecedor = %s,
                        data_compra = %s, data_atualizacao = CURRENT_TIMESTAMP
                    WHERE 
                        codigo_item_catalogo = %s AND 
                        id_compra = %s::VARCHAR AND 
                        ni_fornecedor = %s::VARCHAR;
                """, dados_update)
                atualizados = len(dados_update)
    return novos, atualizados

# --- Funções de API Assíncrona (sem alterações) ---
async def fetch_precos_item(session: aiohttp.ClientSession, codigo_item: str, tipo_item: str) -> Optional[List[Dict[str, Any]]]:
    tipo_normalizado = tipo_item.strip().lower()
    if 'material' in tipo_normalizado:
        base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"
        params = {'codigoItemCatalogo': codigo_item, 'pagina': 1, 'tamanhoPagina': 5000}
        endpoint_usado = "MATERIAL (CSV)"
    elif 'serviço' in tipo_normalizado or 'servico' in tipo_normalizado:
        base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV"
        params = {'codigoItemCatalogo': codigo_item, 'pagina': 1}
        endpoint_usado = "SERVIÇO (CSV)"
    else:
        logging.warning(f"Tipo '{tipo_item}' não reconhecido para item {codigo_item}. Pulando.")
        return None

    logging.info(f"Buscando item {codigo_item} ({endpoint_usado})")
    try:
        async with session.get(base_url, params=params, headers={'accept': 'text/csv'}, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
            if response.status != 200:
                logging.error(f"Erro para item {codigo_item}. Status: {response.status}.")
                return None
            content_bytes = await response.read()
            if not content_bytes: return []
            try: decoded_content = content_bytes.decode('latin-1')
            except UnicodeDecodeError: decoded_content = content_bytes.decode('cp1252', errors='ignore')
            df = pd.read_csv(io.StringIO(decoded_content), sep=';', on_bad_lines='warn')
            if df.empty: return []
            df.columns = [normalizar_nome_coluna(col) for col in df.columns]
            # Força colunas chave para string no PANDAS
            for col in ['numero_compra', 'cnpj_vencedor']:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            df = df.where(pd.notna(df), None)
            precos = df.to_dict('records')
            logging.info(f"SUCESSO: Processados {len(precos)} preços para o item {codigo_item}.")
            return precos
    except Exception as e:
        logging.error(f"Erro fatal ao processar CSV para item {codigo_item}: {e}", exc_info=True)
        return None

# --- Orquestração Principal (sem alterações) ---
async def process_itens_concorrently(conn_string: str, itens: List[Tuple[str, str]]) -> Dict[str, int]:
    stats = {'total_itens': len(itens), 'itens_processados': 0, 'precos_novos': 0, 'precos_atualizados': 0, 'erros': 0}
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async def fetch_and_sync(codigo: str, tipo: str) -> None:
        async with semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    precos = await fetch_precos_item(session, codigo, tipo)
                    if precos is not None:
                        try:
                            novos, atualizados = sync_precos_catalogo(conn_string, codigo, tipo, precos)
                            stats['precos_novos'] += novos
                            stats['precos_atualizados'] += atualizados
                            stats['itens_processados'] += 1
                            if novos > 0 or atualizados > 0:
                                logging.info(f"Item {codigo}: Sincronizado ({novos} novos, {atualizados} atualizados).")
                        except Exception as db_error:
                            logging.error(f"Erro de banco de dados para item {codigo}: {db_error}")
                            stats['erros'] += 1
                    else:
                        stats['erros'] += 1
            except Exception as e:
                logging.error(f"Erro crítico no fluxo do item {codigo}: {e}", exc_info=True)
                stats['erros'] += 1
    tasks = [fetch_and_sync(codigo, tipo) for codigo, tipo in itens]
    await asyncio.gather(*tasks)
    return stats

async def main():
    conn = get_db_connection()
    if not conn:
        logging.error("Não foi possível estabelecer a conexão inicial com o banco. Abortando.")
        return
    try:
        itens_para_buscar = get_itens_catalogo_from_db(conn)
        if not itens_para_buscar:
            logging.info("Nenhum item encontrado no banco para processar.")
            return
    finally:
        conn.close()
    stats = await process_itens_concorrently(CONN_STRING, itens_para_buscar)
    logging.info("=" * 60)
    logging.info("RESUMO FINAL DA SINCRONIZAÇÃO")
    logging.info(f"Total de itens únicos no banco: {stats['total_itens']}")
    logging.info(f"Itens processados (chamadas à API): {stats['itens_processados']}")
    logging.info(f"Novos registros de preço inseridos: {stats['precos_novos']}")
    logging.info(f"Registros de preço existentes atualizados: {stats['precos_atualizados']}")
    logging.info(f"Itens com erro no processamento: {stats['erros']}")
    logging.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())