import asyncio
import aiohttp
import argparse
import pandas as pd
import logging
import os
import re

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configurações ---
MAX_CONCURRENT_REQUESTS = 3  # Limite de requisições concorrentes
REQUEST_DELAY_SECONDS = 1  # Atraso em segundos entre as requisições de um mesmo worker

# Colunas que podem conter valores monetários para formatação
MONEY_COLUMNS = [
    'valorUnitario', 'valorTotal', 'valorEstimado', 'valorUnitarioEstimado',
    'valorTotalHomologado', 'valorUnitarioHomologado', 'valorSubscrito',
    'valorOriginal', 'valorNegociado', 'valorAtualizado', 'valorItem', 'valorTotalResultado',
    'valorUnitarioResultado' # Adicionando esta que pode aparecer nos resultados
]

# Mapeamento dos endpoints da API
API_ENDPOINTS = {
    'CONTRATACAO': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id',
        'param_name': 'codigo',
        'is_json': True
    },
    'ITENS': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id',
        'param_name': 'codigo',
        'is_json': True
    },
    'RESULTADOS': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id',
        'param_name': 'codigo',
        'is_json': True
    },
    'PRECOS_MATERIAL': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV',
        'param_name': 'codigoItemCatalogo',
        'is_json': False
    },
    'PRECOS_SERVICO': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV',
        'param_name': 'codigoItemCatalogo',
        'is_json': False
    }
}

async def fetch(session, url, item_id, semaphore):
    """
    Realiza uma requisição HTTP GET assíncrona.
    """
    async with semaphore:
        try:
            logger.info(f"Iniciando requisição para ID: {item_id} na URL: {url}")
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                if 'application/json' in response.content_type:
                    return item_id, await response.json()
                else:
                    return item_id, await response.text()
        except aiohttp.ClientError as e:
            logger.error(f"Erro na requisição para ID {item_id} na URL {url}: {e}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout na requisição para ID {item_id} na URL {url}.")
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar dados para ID {item_id} na URL {url}: {e}")
        return item_id, None

def read_ids_from_file(filepath):
    """
    Lê IDs de um arquivo de texto, um por linha.
    """
    ids = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                item_id = line.strip()
                if item_id:
                    ids.append(item_id)
        logger.info(f"Lidos {len(ids)} IDs do arquivo '{filepath}'.")
    except FileNotFoundError:
        logger.error(f"Erro: O arquivo '{filepath}' não foi encontrado.")
        raise
    except Exception as e:
        logger.error(f"Erro ao ler IDs do arquivo '{filepath}': {e}")
        raise
    return ids

def format_money_columns(df):
    """
    Formata colunas monetárias para o padrão brasileiro (4 casas decimais).
    """
    for col in MONEY_COLUMNS:
        if col in df.columns:
            # Converte a coluna para numérico, forçando erros para NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Aplica a formatação condicionalmente para valores não nulos
            df[col] = df[col].apply(
                lambda x: f"{x:,.4f}".replace(",", "TEMP_SEP").replace(".", ",").replace("TEMP_SEP", ".")
                if pd.notna(x) else None
            )
    return df

async def producer(url_queue, ids_to_process, endpoint_info):
    """
    Gera URLs para serem processadas e as coloca na fila.
    """
    url_base = endpoint_info['url_base']
    param_name = endpoint_info['param_name']
    is_json = endpoint_info['is_json']

    for item_id in ids_to_process:
        if is_json:
            url = f"{url_base}?tipo=idCompra&{param_name}={item_id}"
        else: # CSV APIs, com paginação e tamanho de página maior
            # Para CSV, vamos buscar uma página grande para reduzir o número total de requisições
            # Nota: a API do PNCP para CSV retorna apenas uma página por requisição,
            # sem metadados de paginação como 'totalPaginas'.
            # A estratégia aqui é apenas aumentar o tamanho da página para cobrir a maioria dos casos.
            url = f"{url_base}?{param_name}={item_id}&pagina=1&tamanhoPagina=1000"
        
        await url_queue.put({'id': item_id, 'url': url, 'is_json': is_json})

async def worker(worker_id, url_queue, results, semaphore, session):
    """
    Processa URLs da fila, busca dados e os adiciona aos resultados.
    """
    while True:
        try:
            task = await url_queue.get()
            item_id = task['id']
            url = task['url']
            is_json = task['is_json']

            data = await fetch(session, url, item_id, semaphore)
            if data is not None:
                results.append((item_id, data[1], is_json))
            
            url_queue.task_done()
            await asyncio.sleep(REQUEST_DELAY_SECONDS) # Atraso para reduzir a frequência de requisições

        except asyncio.CancelledError:
            break # Sair se a tarefa for cancelada (ao fechar o worker)
        except Exception as e:
            logger.error(f"Erro no worker {worker_id} ao processar ID {item_id}: {e}")
            url_queue.task_done() # Garantir que a tarefa seja marcada como feita mesmo em caso de erro

async def collect_data(api_name, ids_to_process, semaphore):
    """
    Coleta dados da API especificada para uma lista de IDs usando um padrão produtor-consumidor.
    """
    endpoint_info = API_ENDPOINTS.get(api_name)
    if not endpoint_info:
        logger.error(f"API '{api_name}' não reconhecida.")
        return None

    url_queue = asyncio.Queue()
    raw_results = [] # Lista para armazenar (item_id, data, is_json)
    
    async with aiohttp.ClientSession() as session:
        # Criar produtor
        producer_task = asyncio.create_task(producer(url_queue, ids_to_process, endpoint_info))

        # Criar consumidores (workers)
        workers = []
        for i in range(MAX_CONCURRENT_REQUESTS):
            worker_task = asyncio.create_task(worker(i, url_queue, raw_results, semaphore, session))
            workers.append(worker_task)

        # Esperar o produtor terminar
        await producer_task
        # Esperar todas as tarefas da fila serem concluídas
        await url_queue.join()

        # Cancelar tarefas dos workers (para que saiam de seus loops 'while True')
        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True) # Esperar que os workers sejam cancelados

    all_dfs = []
    all_csv_texts = []
    header_written = False

    for item_id, data, is_json_flag in raw_results:
        if data is None:
            continue

        if is_json_flag:
            if isinstance(data, dict) and 'resultado' in data:
                try:
                    df_response = pd.json_normalize(data['resultado'])
                    if not df_response.empty:
                        # Adiciona uma coluna para o ID original, se ainda não existir
                        if 'idCompra_original' not in df_response.columns:
                            df_response['idCompra_original'] = item_id
                        all_dfs.append(df_response)
                except Exception as e:
                    logger.error(f"Erro ao normalizar dados JSON para ID {item_id}: {e}")
            else:
                logger.warning(f"Resposta JSON para ID {item_id} não contém a chave 'resultado' ou não é um dicionário.")
        else: # CSV
            # O processamento de CSV é feito separadamente para não misturar headers
            # Consideramos que o primeiro resultado CSV válido contém o cabeçalho
            if not header_written:
                all_csv_texts.append(data)
                header_written = True
            else:
                lines = data.strip().split('\n')
                if len(lines) > 1: # Ignora o cabeçalho se houver e adiciona o resto
                    all_csv_texts.append('\n'.join(lines[1:]))

    if endpoint_info['is_json']:
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True, join='outer')
            final_df = format_money_columns(final_df)
            return final_df
        else:
            logger.warning("Nenhum DataFrame foi gerado a partir dos dados JSON.")
            return pd.DataFrame()
    else: # Retorna texto CSV concatenado
        if all_csv_texts:
            return '\n'.join(all_csv_texts)
        else:
            logger.warning("Nenhum texto CSV foi coletado.")
            return ""

async def main():
    """
    Função principal para parsear argumentos, coletar dados e salvar.
    """
    parser = argparse.ArgumentParser(description="Coleta dados do PNCP.")
    parser.add_argument('--api', type=str, required=True, choices=API_ENDPOINTS.keys(), help="Tipo de API a ser consultada.")
    parser.add_argument('--output', type=str, required=True, help="Nome do arquivo de saída (ex: dados.csv).")
    args = parser.parse_args()

    ids_filepath = 'id_lista.txt'
    try:
        ids_to_process = read_ids_from_file(ids_filepath)
    except (FileNotFoundError, Exception):
        return

    if not ids_to_process:
        logger.warning("Nenhum ID válido para processar. Encerrando.")
        return

    logger.info(f"Iniciando coleta de dados para a API: {args.api}")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    data_result = await collect_data(args.api, ids_to_process, semaphore)

    output_filename = args.output

    if data_result is not None:
        if isinstance(data_result, pd.DataFrame):
            if not data_result.empty:
                try:
                    data_result.to_csv(output_filename, index=False, encoding='utf-8-sig', sep=';')
                    logger.info(f"Dados coletados salvos em '{output_filename}'.")
                except Exception as e:
                    logger.error(f"Erro ao salvar DataFrame em '{output_filename}': {e}")
            else:
                logger.warning("Nenhum dado foi coletado para salvar.")
        elif isinstance(data_result, str) and data_result.strip():
            try:
                with open(output_filename, 'w', encoding='utf-8-sig') as f:
                    f.write(data_result)
                logger.info(f"Dados CSV brutos salvos em '{output_filename}'.")
            except Exception as e:
                logger.error(f"Erro ao salvar texto CSV em '{output_filename}': {e}")
    else:
        logger.warning(f"Nenhum dado foi coletado ou gerado para salvar no arquivo '{output_filename}'.")

    logger.info("Coleta de dados concluída.")

if __name__ == '__main__':
    asyncio.run(main())
