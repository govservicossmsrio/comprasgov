# Arquivo: atualizar_contratacoes.py (Versão Corrigida)

import os
import aiohttp
import asyncio
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
API_BASE_URL = "https://dadosabertos.compras.gov.br/modulo-contratacoes"
CONCURRENT_REQUESTS_LIMIT = 10

# ==============================================================================
# SEÇÃO CORRIGIDA: FUNÇÕES DE BANCO DE DADOS ADICIONADAS AQUI
# ==============================================================================

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    if not CONN_STRING:
        logging.error("String de conexão COCKROACHDB_CONN_STRING não encontrada.")
        return None
    try:
        conn = psycopg2.connect(CONN_STRING)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def get_compra_update_date(conn, id_compra):
    """Busca a data de atualização de uma compra no banco."""
    with conn.cursor() as cur:
        cur.execute("SELECT data_atualizacao_pncp FROM compras WHERE id = %s", (id_compra,))
        result = cur.fetchone()
        return result[0] if result else None

def upsert_data(conn, table, columns, data, conflict_target, update_columns):
    """
    Função genérica para inserir ou atualizar dados (UPSERT).
    """
    if not data:
        return 0
        
    cols_str = ", ".join(columns)
    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
    conflict_str = ", ".join(conflict_target)
    
    query = f"""
        INSERT INTO {table} ({cols_str})
        VALUES %s
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};
    """
    
    with conn.cursor() as cur:
        extras.execute_values(cur, query, data)
        conn.commit()
        return cur.rowcount

# ==============================================================================
# FIM DA SEÇÃO CORRIGIDA
# ==============================================================================


# --- Funções de API Assíncronas ---

async def fetch_api_data_async(session, endpoint, params):
    """Busca dados de um endpoint da API de forma assíncrona."""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json(content_type=None)
    except aiohttp.ClientError as e:
        logging.error(f"Erro de cliente ao chamar a API {endpoint} com params {params}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado em fetch_api_data_async para {url}: {e}")
        return None


# --- Funções de Processamento Assíncronas ---

async def processar_contratacao_async(session, semaphore, conn, id_compra):
    """
    Processa uma única contratação de forma assíncrona e controlada pelo semáforo.
    """
    async with semaphore:
        logging.info(f"Processando idCompra: {id_compra}")
        
        db_update_date = get_compra_update_date(conn, id_compra)
        
        params = {'tipo': 'idCompra', 'codigo': id_compra}
        contratacao_json = await fetch_api_data_async(session, "1.1_consultarContratacoes_PNCP_14133_Id", params)
        
        if not contratacao_json or not contratacao_json.get('resultado'):
            logging.warning(f"Nenhuma contratação encontrada para idCompra: {id_compra}")
            return

        compra = contratacao_json['resultado'][0]
        api_update_date_str = compra.get('dataAtualizacaoPncp')
        
        try:
            api_update_date = datetime.fromisoformat(api_update_date_str.replace('Z', '+00:00')) if api_update_date_str else None
        except (ValueError, TypeError):
            api_update_date = None

        if db_update_date and api_update_date and db_update_date >= api_update_date:
            logging.info(f"Dados para idCompra {id_compra} já estão atualizados. Pulando.")
            return

        logging.info(f"Atualização necessária para idCompra {id_compra}. Buscando sub-dados...")

        itens_task = fetch_api_data_async(session, "2.1_consultarItensContratacoes_PNCP_14133_Id", params)
        resultados_task = fetch_api_data_async(session, "3.1_consultarResultadoItensContratacoes_PNCP_14133_Id", params)
        
        itens_json, resultados_json = await asyncio.gather(itens_task, resultados_task)
        
        # Aqui virá a lógica de persistência, que também usará a função upsert_data
        logging.info(f"Dados para idCompra {id_compra} foram coletados. (Lógica de persistência a ser implementada).")


# --- Função Principal Assíncrona ---

async def main_async():
    """Função principal que orquestra o processo de forma assíncrona."""
    conn = get_db_connection() # Esta linha agora funcionará
    if not conn:
        return

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
    
    async with aiohttp.ClientSession() as session:
        try:
            with open('idCompra_lista.txt', 'r') as f:
                id_compras = [line.strip() for line in f if line.strip()]
            
            logging.info(f"Encontrados {len(id_compras)} IDs de compra para processar.")
            
            tasks = [processar_contratacao_async(session, semaphore, conn, id_compra) for id_compra in id_compras]
            
            await asyncio.gather(*tasks)
                
        except FileNotFoundError:
            logging.error("Arquivo 'idCompra_lista.txt' não encontrado.")
        finally:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    asyncio.run(main_async())