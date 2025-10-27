import os
import requests # Manteremos para o código síncrono, se houver
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
# Limite de requisições concorrentes. Comece com um valor baixo (5-10) e aumente com cuidado.
CONCURRENT_REQUESTS_LIMIT = 10 

# --- Funções de Banco de Dados (permanecem as mesmas) ---
# get_db_connection, get_compra_update_date, upsert_data...

# --- Novas Funções de API Assíncronas ---

async def fetch_api_data_async(session, endpoint, params):
    """Busca dados de um endpoint da API de forma assíncrona."""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json(content_type=None) # content_type=None lida com headers incorretos
    except aiohttp.ClientError as e:
        logging.error(f"Erro de cliente ao chamar a API {endpoint} com params {params}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado em fetch_api_data_async para {url}: {e}")
        return None


# --- Novas Funções de Processamento Assíncronas ---

async def processar_contratacao_async(session, semaphore, conn, id_compra):
    """
    Processa uma única contratação de forma assíncrona e controlada pelo semáforo.
    """
    async with semaphore: # Espera por um "espaço" livre no semáforo
        logging.info(f"Processando idCompra: {id_compra}")
        
        # 1. Verificar data no banco (operação síncrona, mas rápida)
        db_update_date = get_compra_update_date(conn, id_compra)
        
        # 2. Buscar dados da contratação principal
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

        # 3. Verificar se a atualização é necessária
        if db_update_date and api_update_date and db_update_date >= api_update_date:
            logging.info(f"Dados para idCompra {id_compra} já estão atualizados. Pulando.")
            return

        logging.info(f"Atualização necessária para idCompra {id_compra}. Buscando sub-dados...")

        # 4. Buscar itens e resultados de forma concorrente
        itens_task = fetch_api_data_async(session, "2.1_consultarItensContratacoes_PNCP_14133_Id", params)
        resultados_task = fetch_api_data_async(session, "3.1_consultarResultadoItensContratacoes_PNCP_14133_Id", params)
        
        # Espera os dois terminarem
        itens_json, resultados_json = await asyncio.gather(itens_task, resultados_task)
        
        # 5. Persistir os dados no banco (operação síncrona)
        # A lógica de UPSERT permanece a mesma, mas agora você a chama com os dados coletados
        # Exemplo simplificado:
        # upsert_orgao(conn, compra)
        # upsert_unidade(conn, compra)
        # upsert_compra(conn, compra)
        # if itens_json and itens_json.get('resultado'):
        #     upsert_itens(conn, itens_json['resultado'])
        # if resultados_json and resultados_json.get('resultado'):
        #     upsert_resultados(conn, resultados_json['resultado'])
            
        logging.info(f"Dados para idCompra {id_compra} foram coletados e enviados para persistência.")


# --- Nova Função Principal Assíncrona ---

async def main_async():
    """Função principal que orquestra o processo de forma assíncrona."""
    conn = get_db_connection()
    if not conn:
        return

    # Cria o semáforo com o limite definido
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
    
    # Cria uma única sessão aiohttp para reutilizar conexões (mais eficiente)
    async with aiohttp.ClientSession() as session:
        try:
            with open('idCompra_lista.txt', 'r') as f:
                id_compras = [line.strip() for line in f if line.strip()]
            
            logging.info(f"Encontrados {len(id_compras)} IDs de compra para processar.")
            
            # Cria uma lista de tarefas, uma para cada id_compra
            tasks = [processar_contratacao_async(session, semaphore, conn, id_compra) for id_compra in id_compras]
            
            # Executa todas as tarefas concorrentemente
            await asyncio.gather(*tasks)
                
        except FileNotFoundError:
            logging.error("Arquivo 'idCompra_lista.txt' não encontrado.")
        finally:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    # Inicia o loop de eventos do asyncio para rodar a função main_async
    asyncio.run(main_async())
