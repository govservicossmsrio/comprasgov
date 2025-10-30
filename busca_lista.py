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

# Colunas que podem conter valores monetários para formatação
MONEY_COLUMNS = [
    'valorUnitario', 'valorTotal', 'valorEstimado', 'valorUnitarioEstimado',
    'valorTotalHomologado', 'valorUnitarioHomologado', 'valorSubscrito',
    'valorOriginal', 'valorNegociado', 'valorAtualizado', 'valorItem',
    'valorTotalResultado', 'valorUnitarioResultado', # Adicionado para cobrir os exemplos
    'precoUnitario', 'precoTotal' # Potenciais colunas das APIs CSV de preços
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
    Realiza uma requisição HTTP GET assíncrona com controle de concorrência.
    Retorna o item_id e o conteúdo da resposta (JSON decodificado ou texto bruto).
    """
    async with semaphore: # Limita requisições concorrentes
        try:
            logger.info(f"Iniciando requisição para ID: {item_id} na URL: {url}")
            async with session.get(url, timeout=30) as response:
                response.raise_for_status() # Lança HTTPError para status de erro (4xx, 5xx)
                if 'application/json' in response.content_type:
                    return item_id, await response.json()
                else: # Assume-se que é CSV para outros content_types
                    return item_id, await response.text()
        except aiohttp.ClientError as e:
            logger.error(f"Erro de cliente HTTP para ID {item_id} na URL {url}: {e}")
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
    Formata colunas monetárias para o padrão brasileiro (4 casas decimais),
    tratando valores nulos e strings para evitar erros.
    """
    if df.empty:
        return df

    for col in MONEY_COLUMNS:
        if col in df.columns:
            # Converte a coluna para numérico, forçando erros para NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Aplica a formatação condicionalmente para valores não nulos
            # Substitui o separador decimal '.' por ',' e o separador de milhar ',' por '.'
            df[col] = df[col].apply(
                lambda x: f"{x:,.4f}".replace(",", "TEMP_SEP").replace(".", ",").replace("TEMP_SEP", ".")
                if pd.notna(x) else None
            )
    return df

async def producer(id_list, api_name, url_queue):
    """
    Função produtor que gera URLs para a fila de processamento.
    """
    endpoint_info = API_ENDPOINTS[api_name]
    url_base = endpoint_info['url_base']
    param_name = endpoint_info['param_name']
    is_json = endpoint_info['is_json']

    for item_id in id_list:
        if is_json:
            # URLs JSON sempre usam 'tipo=idCompra' e o 'codigo'
            url = f"{url_base}?tipo=idCompra&{param_name}={item_id}"
        else:
            # URLs CSV usam 'tamanhoPagina=1000' (para pegar uma página massiva)
            url = f"{url_base}?{param_name}={item_id}&pagina=1&tamanhoPagina=1000"
        
        await url_queue.put((url, item_id, is_json))
    
    logger.info("Produtor terminou de adicionar URLs à fila.")

async def worker(url_queue, results_queue, semaphore, session):
    """
    Função consumidor (worker) que processa URLs da fila.
    """
    while True:
        try:
            url, item_id, is_json = await url_queue.get()
            fetched_item_id, fetched_data = await fetch(session, url, item_id, semaphore)
            await results_queue.put((fetched_item_id, fetched_data, is_json))
            url_queue.task_done()
        except asyncio.CancelledError:
            # Sai do loop quando a tarefa é cancelada
            break
        except Exception as e:
            # Loga o erro, mas continua processando para não travar a fila
            logger.error(f"Erro inesperado no worker ao processar: {e}")
            # Garante que a tarefa seja marcada como concluída mesmo em caso de erro
            if not url_queue.empty(): # Evita erro se a fila já estiver vazia ao pegar
                url_queue.task_done()

async def main():
    """
    Função principal para parsear argumentos, orquestrar coleta de dados e salvar.
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
    url_queue = asyncio.Queue()
    results_queue = asyncio.Queue()
    
    # Inicia a tarefa do produtor para popular a fila de URLs
    producer_task = asyncio.create_task(producer(ids_to_process, args.api, url_queue))

    # Inicia as tarefas dos consumidores (workers)
    async with aiohttp.ClientSession() as session:
        workers = [
            asyncio.create_task(worker(url_queue, results_queue, semaphore, session))
            for _ in range(MAX_CONCURRENT_REQUESTS)
        ]

        # Espera o produtor terminar de adicionar todas as URLs
        await producer_task
        # Espera que todas as tarefas na fila de URLs sejam processadas pelos workers
        await url_queue.join()

        # Cancela os workers que estão em loop infinito
        for w in workers:
            w.cancel()
        # Espera que os workers sejam cancelados, ignorando possíveis exceções de cancelamento
        await asyncio.gather(*workers, return_exceptions=True)

    # --- Processamento e Consolidação dos Resultados ---
    all_dfs = []
    all_csv_texts = []
    header_written = False

    # Esvazia a fila de resultados e processa cada item
    while not results_queue.empty():
        item_id, data, is_json = await results_queue.get()
        if data is None:
            continue

        if is_json:
            # Processa respostas JSON
            if isinstance(data, dict) and 'resultado' in data and data['resultado'] is not None:
                try:
                    df_response = pd.json_normalize(data['resultado'])
                    if not df_response.empty:
                       # Opcional: Adicionar o ID original para rastreabilidade
                       # df_response['id_original_busca'] = item_id 
                       all_dfs.append(df_response)
                except Exception as e:
                    logger.error(f"Erro ao normalizar dados JSON para ID {item_id}: {e}")
            else:
                logger.warning(f"Resposta JSON para ID {item_id} não contém dados válidos em 'resultado' ou 'resultado' é nulo.")
        else: # Processa respostas CSV
            # O primeiro bloco de CSV inclui o cabeçalho, os subsequentes não
            if not header_written:
                all_csv_texts.append(data.strip()) # strip para remover quebras de linha extras
                header_written = True
            else:
                lines = data.strip().split('\n')
                if len(lines) > 1: # Se tiver mais que o cabeçalho (que não queremos)
                    all_csv_texts.append('\n'.join(lines[1:]))

    output_filename = args.output
    
    # Salva os resultados
    if API_ENDPOINTS[args.api]['is_json']:
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True, join='outer')
            final_df = format_money_columns(final_df)
            if not final_df.empty:
                try:
                    final_df.to_csv(output_filename, index=False, encoding='utf-8-sig', sep=';')
                    logger.info(f"Dados JSON coletados e salvos em '{output_filename}'.")
                except Exception as e:
                    logger.error(f"Erro ao salvar DataFrame em '{output_filename}': {e}")
            else:
                logger.warning("Nenhum dado foi coletado dos endpoints JSON para salvar.")
        else:
            logger.warning("Nenhum DataFrame foi gerado a partir dos dados JSON.")
    else: # API CSV
        if all_csv_texts:
            # Junta todos os textos CSV com uma nova linha.
            # Se o primeiro texto já tem cabeçalho, os demais são só dados.
            final_csv_text = '\n'.join(all_csv_texts)
            if final_csv_text.strip(): # Verifica se não está vazio após o strip
                try:
                    with open(output_filename, 'w', encoding='utf-8-sig') as f:
                        f.write(final_csv_text)
                    logger.info(f"Dados CSV brutos coletados e salvos em '{output_filename}'.")
                except Exception as e:
                    logger.error(f"Erro ao salvar texto CSV em '{output_filename}': {e}")
            else:
                 logger.warning("Nenhum dado CSV foi coletado para salvar.")
        else:
            logger.warning("Nenhum texto CSV foi coletado.")
    
    logger.info("Coleta de dados concluída.")

if __name__ == '__main__':
    asyncio.run(main())
