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
    """
    Converte um nome de coluna para snake_case.
    Ex: 'Valor Unitário Homologado' -> 'valor_unitario_homologado'
    """
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

# --- Funções de Banco de Dados ---

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    try:
        conn = psycopg2.connect(CONN_STRING)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def get_itens_catalogo_from_db(conn) -> List[Tuple[str, str]]:
    """Busca todos os itens de catálogo distintos do banco."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT
                codigo,
                TRIM(LOWER(tipo)) as tipo
            FROM itens_catalogo
            WHERE codigo IS NOT NULL AND tipo IS NOT NULL
            ORDER BY codigo;
        """)
        itens = cur.fetchall()
        logging.info(f"Encontrados {len(itens)} itens únicos no banco de dados para processar.")
        return itens

def sync_precos_catalogo(
    conn,
    codigo_item: str,
    tipo_item: str,
    precos_api: List[Dict[str, Any]]
) -> Tuple[int, int]:
    """
    Sincroniza os preços da API com o banco, com correção de tipo de dados.
    Retorna (novos_registros, registros_atualizados).
    """
    if not precos_api:
        return 0, 0

    precos_db_map = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT * FROM precos_catalogo WHERE codigo_item_catalogo = %s",
            (codigo_item,)
        )
        for row in cur:
            # Garante que as chaves sejam strings para comparação consistente
            chave = f"{str(row['id_compra'])}-{str(row['ni_fornecedor'])}"
            precos_db_map[chave] = row

    precos_para_inserir = []
    precos_para_atualizar = []

    for p in precos_api:
        # Garante que os valores usados na chave sejam strings
        id_compra_api = str(p.get('numero_compra', ''))
        ni_fornecedor_api = str(p.get('cnpj_vencedor', ''))

        if not id_compra_api or not ni_fornecedor_api:
            continue

        chave_api = f"{id_compra_api}-{ni_fornecedor_api}"
        
        try:
            valor_unitario_api = float(p.get('valor_unitario_homologado', 0))
        except (ValueError, TypeError):
            continue

        preco_existente = precos_db_map.get(chave_api)

        if not preco_existente:
            precos_para_inserir.append(p)
        else:
            if not psycopg2.extensions.Float(valor_unitario_api).isclose(preco_existente['valor_unitario']):
                precos_para_atualizar.append(p)

    with conn.cursor() as cur:
        # Lote de Inserção
        if precos_para_inserir:
            insert_query = """
                INSERT INTO precos_catalogo (
                    codigo_item_catalogo, tipo_item, descricao_item, unidade_medida,
                    quantidade_total, valor_unitario, valor_total, ni_fornecedor,
                    nome_fornecedor, id_compra, data_compra, data_atualizacao
                ) VALUES %s
            """
            dados_insert = [
                (
                    codigo_item, tipo_item, p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'),
                    p.get('quantidade_item'), p.get('valor_unitario_homologado'), p.get('valor_total_homologado'),
                    str(p.get('cnpj_vencedor')), str(p.get('nome_vencedor')), str(p.get('numero_compra')),
                    p.get('data_resultado'), datetime.now()
                ) for p in precos_para_inserir
            ]
            psycopg2.extras.execute_values(cur, insert_query, dados_insert)

        # Lote de Atualização com execute_batch
        if precos_para_atualizar:
            update_query = """
                UPDATE precos_catalogo SET
                    descricao_item = %s,
                    unidade_medida = %s,
                    quantidade_total = %s,
                    valor_unitario = %s,
                    valor_total = %s,
                    nome_fornecedor = %s,
                    data_compra = %s,
                    data_atualizacao = CURRENT_TIMESTAMP
                WHERE codigo_item_catalogo = %s AND id_compra = %s AND ni_fornecedor = %s;
            """
            dados_update = [
                (
                    p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'),
                    p.get('quantidade_item'), p.get('valor_unitario_homologado'),
                    p.get('valor_total_homologado'), str(p.get('nome_vencedor')),
                    p.get('data_resultado'),
                    codigo_item, str(p.get('numero_compra')), str(p.get('cnpj_vencedor'))
                ) for p in precos_para_atualizar
            ]
            psycopg2.extras.execute_batch(cur, update_query, dados_update)

    conn.commit()
    return len(precos_para_inserir), len(precos_para_atualizar)

# --- Funções de API Assíncrona ---

async def fetch_precos_item(
    session: aiohttp.ClientSession,
    codigo_item: str,
    tipo_item: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Busca os preços de um item da API correta (Material/Serviço), processa o CSV
    e o converte para uma lista de dicionários.
    """
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
            if not content_bytes:
                logging.info(f"Nenhum preço encontrado para item {codigo_item} (resposta vazia).")
                return []

            try:
                decoded_content = content_bytes.decode('latin-1')
            except UnicodeDecodeError:
                decoded_content = content_bytes.decode('cp1252', errors='ignore')
            
            df = pd.read_csv(io.StringIO(decoded_content), sep=';', on_bad_lines='warn')

            if df.empty:
                logging.info(f"Nenhum preço encontrado para item {codigo_item} (CSV vazio).")
                return []

            df.columns = [normalizar_nome_coluna(col) for col in df.columns]
            df = df.where(pd.notna(df), None)
            precos = df.to_dict('records')

            logging.info(f"SUCESSO: Encontrados e processados {len(precos)} preços para o item {codigo_item}.")
            return precos

    except asyncio.TimeoutError:
        logging.error(f"Timeout ao buscar CSV para item {codigo_item}")
        return None
    except Exception as e:
        logging.error(f"Erro fatal ao processar CSV para item {codigo_item}: {e}", exc_info=True)
        return None

# --- Orquestração Principal ---

async def process_itens_concorrently(
    itens: List[Tuple[str, str]],
    conn: psycopg2.extensions.connection
) -> Dict[str, int]:
    stats = {
        'total_itens': len(itens),
        'itens_processados': 0,
        'precos_novos': 0,
        'precos_atualizados': 0,
        'erros': 0
    }
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_and_sync(codigo: str, tipo: str) -> None:
        async with semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    precos = await fetch_precos_item(session, codigo, tipo)
                    if precos is not None:
                        # Cada tarefa agora tem seu próprio cursor para evitar conflitos de transação
                        with conn.cursor() as cur_task:
                            try:
                                novos, atualizados = sync_precos_catalogo(conn, codigo, tipo, precos)
                                stats['precos_novos'] += novos
                                stats['precos_atualizados'] += atualizados
                                stats['itens_processados'] += 1
                                if novos > 0 or atualizados > 0:
                                    logging.info(f"Item {codigo}: Sincronizado ({novos} novos, {atualizados} atualizados).")
                                conn.commit() # Commit por tarefa bem-sucedida
                            except Exception as db_error:
                                logging.error(f"Erro de banco de dados para item {codigo}: {db_error}")
                                conn.rollback() # Rollback em caso de erro na tarefa
                                stats['erros'] += 1
                    else:
                        stats['erros'] += 1
            except Exception as e:
                logging.error(f"Erro crítico no fluxo do item {codigo}: {e}", exc_info=True)
                stats['erros'] += 1

    # O código original já passava a conexão, mas a gestão de transação foi movida para dentro da tarefa
    tasks = [fetch_and_sync(codigo, tipo) for codigo, tipo in itens]
    await asyncio.gather(*tasks)
    return stats

async def main():
    """Função principal que orquestra todo o processo."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        itens_para_buscar = get_itens_catalogo_from_db(conn)
        if not itens_para_buscar:
            logging.info("Nenhum item encontrado no banco para processar.")
            return
        
        stats = await process_itens_concorrently(itens_para_buscar, conn)
        
        logging.info("=" * 60)
        logging.info("RESUMO FINAL DA SINCRONIZAÇÃO")
        logging.info(f"Total de itens únicos no banco: {stats['total_itens']}")
        logging.info(f"Itens processados (chamadas à API): {stats['itens_processados']}")
        logging.info(f"Novos registros de preço inseridos: {stats['precos_novos']}")
        logging.info(f"Registros de preço existentes atualizados: {stats['precos_atualizados']}")
        logging.info(f"Itens com erro no processamento: {stats['erros']}")
        logging.info("=" * 60)

    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    asyncio.run(main())