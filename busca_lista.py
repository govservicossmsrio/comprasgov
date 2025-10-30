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
    'valorOriginal', 'valorNegociado', 'valorAtualizado', 'valorItem', 'valorTotalResultado'
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

async def collect_data(api_name, ids_to_process, semaphore):
    """
    Coleta dados da API especificada para uma lista de IDs.
    """
    endpoint_info = API_ENDPOINTS.get(api_name)
    if not endpoint_info:
        logger.error(f"API '{api_name}' não reconhecida.")
        return None

    url_base = endpoint_info['url_base']
    param_name = endpoint_info['param_name']
    is_json = endpoint_info['is_json']

    all_dfs = []
    all_csv_texts = []
    header_written = False

    async with aiohttp.ClientSession() as session:
        tasks = []
        for item_id in ids_to_process:
            if is_json:
                url = f"{url_base}?tipo=idCompra&{param_name}={item_id}"
            else:
                url = f"{url_base}?{param_name}={item_id}&pagina=1&tamanhoPagina=10"
            tasks.append(fetch(session, url, item_id, semaphore))

        responses = await asyncio.gather(*tasks)

        for item_id, data in responses:
            if data is None:
                continue

            if is_json:
                if isinstance(data, dict) and 'resultado' in data:
                    try:
                        df_response = pd.json_normalize(data['resultado'])
                        if not df_response.empty:
                           df_response['idCompra_original'] = item_id
                           all_dfs.append(df_response)
                    except Exception as e:
                        logger.error(f"Erro ao normalizar dados JSON para ID {item_id}: {e}")
                else:
                    logger.warning(f"Resposta JSON para ID {item_id} não contém a chave 'resultado' ou não é um dicionário.")
            else: # CSV
                if not header_written:
                    all_csv_texts.append(data)
                    header_written = True
                else:
                    lines = data.strip().split('\n')
                    if len(lines) > 1:
                        all_csv_texts.append('\n'.join(lines[1:]))

    if is_json:
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True, join='outer')
            final_df = format_money_columns(final_df)
            return final_df
        else:
            logger.warning("Nenhum DataFrame foi gerado a partir dos dados JSON.")
            return pd.DataFrame()
    else: # CSV
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
