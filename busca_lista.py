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
# Pausas para ser "amigável" com a API
REQUEST_SUCCESS_DELAY = 2  # Pausa de 2s após cada requisição (sucesso ou falha)
REQUEST_ERROR_DELAY = 6    # Pausa adicional de 6s para erros específicos (400, 429, 503)

# Colunas que podem conter valores monetários para formatação
MONEY_COLUMNS = [
    'valorUnitario', 'valorTotal', 'valorEstimado', 'valorUnitarioEstimado',
    'valorTotalHomologado', 'valorUnitarioHomologado', 'valorSubscrito',
    'valorOriginal', 'valorNegociado', 'valorAtualizado', 'valorItem', 'valorTotalResultado',
    'valorUnitarioResultado'
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

async def fetch_sequentially(session, url, item_id):
    """
    Realiza uma única requisição HTTP GET de forma sequencial.
    Retorna o status da requisição e os dados.
    """
    try:
        logger.info(f"Iniciando requisição para ID: {item_id} na URL: {url}")
        async with session.get(url, timeout=60) as response:
            # Verifica se o status indica um erro que merece uma pausa maior
            if response.status in [400, 429, 503]:
                logger.error(f"Erro {response.status} (Bad Request/Too Many Requests/Service Unavailable) para ID {item_id}. O ID será repetido no final.")
                return 'error_retry', None
            
            response.raise_for_status() # Lança exceção para outros erros HTTP (4xx, 5xx)
            
            if 'application/json' in response.content_type:
                return 'success', await response.json()
            else:
                return 'success', await response.text()
    except aiohttp.ClientError as e:
        logger.error(f"Erro de cliente HTTP para ID {item_id}: {e}. O ID será repetido no final.")
        return 'error_retry', None
    except asyncio.TimeoutError:
        logger.error(f"Timeout na requisição para ID {item_id}. O ID será repetido no final.")
        return 'error_retry', None
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar dados para ID {item_id}: {e}. O ID será repetido no final.")
        return 'error_retry', None

def read_ids_from_file(filepath):
    """Lê IDs de um arquivo de texto, um por linha."""
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
    return ids

def format_money_columns(df):
    """Formata colunas monetárias para o padrão brasileiro (4 casas decimais)."""
    for col in MONEY_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(
                lambda x: f"{x:,.4f}".replace(",", "TEMP_SEP").replace(".", ",").replace("TEMP_SEP", ".")
                if pd.notna(x) else None
            )
    return df

async def run_collection_pass(session, ids_to_process, endpoint_info, results_list, failed_ids_list):
    """
    Executa uma passagem de coleta de dados sequencialmente para a lista de IDs fornecida.
    """
    url_base = endpoint_info['url_base']
    param_name = endpoint_info['param_name']
    is_json = endpoint_info['is_json']

    for item_id in ids_to_process:
        if is_json:
            url = f"{url_base}?tipo=idCompra&{param_name}={item_id}"
        else:
            url = f"{url_base}?{param_name}={item_id}&pagina=1&tamanhoPagina=1000"
        
        status, data = await fetch_sequentially(session, url, item_id)

        if status == 'success':
            results_list.append({'id': item_id, 'data': data, 'is_json': is_json})
        elif status == 'error_retry':
            failed_ids_list.append(item_id)
            # Pausa extra para erros que indicam sobrecarga da API
            logger.info(f"Pausa adicional de {REQUEST_ERROR_DELAY} segundos devido a erro.")
            await asyncio.sleep(REQUEST_ERROR_DELAY)
        
        # Pausa padrão após cada requisição
        logger.info(f"Pausa de {REQUEST_SUCCESS_DELAY} segundos antes da próxima requisição.")
        await asyncio.sleep(REQUEST_SUCCESS_DELAY)

async def main():
    """Função principal para orquestrar a coleta de dados."""
    parser = argparse.ArgumentParser(description="Coleta dados do PNCP de forma sequencial e amigável.")
    parser.add_argument('--api', type=str, required=True, choices=API_ENDPOINTS.keys(), help="Tipo de API a ser consultada.")
    parser.add_argument('--output', type=str, required=True, help="Nome do arquivo de saída (ex: dados.csv).")
    args = parser.parse_args()

    ids_filepath = 'id_lista.txt'
    try:
        initial_ids = read_ids_from_file(ids_filepath)
    except FileNotFoundError:
        return

    if not initial_ids:
        logger.warning("Nenhum ID válido para processar. Encerrando.")
        return

    endpoint_info = API_ENDPOINTS[args.api]
    all_results = []
    failed_ids = []

    async with aiohttp.ClientSession() as session:
        # --- Primeira Passagem ---
        logger.info(f"--- INICIANDO PRIMEIRA PASSAGEM DE COLETA PARA {len(initial_ids)} IDs ---")
        await run_collection_pass(session, initial_ids, endpoint_info, all_results, failed_ids)
        logger.info(f"--- PRIMEIRA PASSAGEM CONCLUÍDA. {len(failed_ids)} IDs falharam e serão repetidos. ---")

        # --- Segunda Passagem (Repetição) ---
        if failed_ids:
            retried_ids = list(failed_ids) # Copia a lista para uma nova
            failed_ids.clear() # Limpa a lista original de falhas
            logger.info(f"--- INICIANDO SEGUNDA PASSAGEM PARA OS {len(retried_ids)} IDs QUE FALHARAM ---")
            await run_collection_pass(session, retried_ids, endpoint_info, all_results, failed_ids)
            logger.info("--- SEGUNDA PASSAGEM CONCLUÍDA. ---")
            if failed_ids:
                logger.warning(f"Os seguintes IDs falharam mesmo após a segunda tentativa: {failed_ids}")

    # --- Processamento e Consolidação dos Resultados ---
    if endpoint_info['is_json']:
        all_dfs = []
        for result in all_results:
            if result['is_json'] and isinstance(result['data'], dict) and 'resultado' in result['data']:
                try:
                    df_response = pd.json_normalize(result['data']['resultado'])
                    if not df_response.empty:
                        df_response['idCompra_original'] = result['id']
                        all_dfs.append(df_response)
                except Exception as e:
                    logger.error(f"Erro ao normalizar dados para ID {result['id']}: {e}")
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True, join='outer')
            final_df = format_money_columns(final_df)
            final_df.to_csv(args.output, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"Dados JSON coletados e salvos em '{args.output}'.")
        else:
            logger.warning("Nenhum dado JSON foi coletado com sucesso para salvar.")
    else: # CSV
        all_csv_texts = []
        header_written = False
        for result in all_results:
            if not result['is_json'] and isinstance(result['data'], str):
                if not header_written:
                    all_csv_texts.append(result['data'].strip())
                    header_written = True
                else:
                    lines = result['data'].strip().split('\n')
                    if len(lines) > 1:
                        all_csv_texts.append('\n'.join(lines[1:]))
        
        if all_csv_texts:
            final_csv_text = '\n'.join(all_csv_texts)
            with open(args.output, 'w', encoding='utf-8-sig') as f:
                f.write(final_csv_text)
            logger.info(f"Dados CSV coletados e salvos em '{args.output}'.")
        else:
            logger.warning("Nenhum dado CSV foi coletado com sucesso para salvar.")

    logger.info("Processo de coleta de dados concluído.")

if __name__ == '__main__':
    asyncio.run(main())
