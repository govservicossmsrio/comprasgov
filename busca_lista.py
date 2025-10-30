## Script para Coleta de Dados do PNCP
import asyncio
import aiohttp
import pandas as pd
import argparse
import logging
import os
import re
from io import StringIO

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurações globais
MAX_CONCURRENT_REQUESTS = 3 # Limite de requisições concorrentes

# Mapeamento de endpoints da API
API_ENDPOINTS = {
    "CONTRATACAO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "record_path": ["_embedded", "contratacoes"],
        "meta": ["total", "pagina", "tamanhoPagina"]
    },
    "ITENS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "record_path": ["_embedded", "contratacoes"], # A API de itens também retorna em 'contratacoes'
        "meta": ["total", "pagina", "tamanhoPagina"]
    },
    "RESULTADOS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "record_path": ["_embedded", "contratacoes"], # A API de resultados também retorna em 'contratacoes'
        "meta": ["total", "pagina", "tamanhoPagina"]
    },
    "PRECOS_MATERIAL": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV",
        "param_name": "codigoItemCatalogo",
        "is_csv": True
    },
    "PRECOS_SERVICO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV",
        "param_name": "codigoItemCatalogo",
        "is_csv": True
    }
}

# Colunas para formatação monetária (com 4 casas decimais e separador ,)
MONETARY_COLUMNS = [
    'valorTotal', 'valorEstimado', 'valorUnitario', 'valorUnitarioEstimado',
    'valorJulgado', 'valorHomologado', 'valorFinal', 'valorGlobal'
]

async def fetch_data(session, url, id_value, api_config, semaphore):
    """
    Realiza uma requisição HTTP GET para a URL especificada e retorna os dados.
    """
    async with semaphore: # Limita o número de requisições concorrentes
        try:
            logger.info(f"Iniciando requisição para ID: {id_value} na URL: {url}")
            async with session.get(url, headers={'accept': '*/*'}) as response:
                response.raise_for_status() # Lança exceção para códigos de status HTTP 4xx/5xx

                if api_config.get("is_csv"):
                    return await response.text()
                else:
                    return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Erro na requisição para ID {id_value} ({url}): {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado para ID {id_value} ({url}): {e}")
            return None

def read_ids_from_file(filepath="id_lista.txt"):
    """
    Lê IDs de um arquivo de texto, um ID por linha.
    Retorna uma lista de IDs válidos.
    """
    if not os.path.exists(filepath):
        logger.error(f"Arquivo '{filepath}' não encontrado. Crie um arquivo com IDs, um por linha.")
        return []

    ids = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_id = line.strip()
                if stripped_id and stripped_id.isdigit(): # Garante que o ID não está vazio e é numérico
                    ids.append(stripped_id)
                elif stripped_id:
                    logger.warning(f"Ignorando linha inválida no arquivo '{filepath}': '{line.strip()}' (não é um ID numérico válido)")
        logger.info(f"Total de {len(ids)} IDs lidos de '{filepath}'.")
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo '{filepath}': {e}")
    return ids

def format_monetary_columns(df):
    """
    Formata colunas monetárias para o padrão brasileiro (4 casas decimais, vírgula como separador).
    """
    if df.empty:
        return df

    for col in MONETARY_COLUMNS:
        if col in df.columns:
            # Tenta converter para numérico, preenchendo erros com NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Remove NaN's para evitar erro na formatação e aplica a formatação
            # O .fillna('') é para que valores NaN não virem 'nan' string
            df[col] = df[col].apply(lambda x: f'{x:,.4f}'.replace('.', 'TEMP').replace(',', '.').replace('TEMP', ',') if pd.notna(x) else '')
    return df

async def collect_data(api_name, output_filename, ids_list):
    """
    Coleta dados da API especificada e salva em um arquivo CSV.
    """
    if api_name not in API_ENDPOINTS:
        logger.error(f"Tipo de API '{api_name}' inválido. Opções válidas: {list(API_ENDPOINTS.keys())}")
        return

    api_config = API_ENDPOINTS[api_name]
    all_dataframes = [] # Para armazenar DataFrames de JSON
    all_csv_texts = [] # Para armazenar textos CSV

    # CRÍTICO: Semaphore deve ser criado dentro do loop de eventos
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for id_value in ids_list:
            # Constrói a URL para a API de contratações/itens/resultados
            if not api_config.get("is_csv"):
                url = f"{api_config['url_base']}?tipo=idCompra&{api_config['param_name']}={id_value}"
            # Constrói a URL para a API de preços (material/serviço)
            else:
                # APIs de preço requerem paginação e tamanhoPagina
                url = f"{api_config['url_base']}?pagina=1&tamanhoPagina=10&{api_config['param_name']}={id_value}"

            tasks.append(fetch_data(session, url, id_value, api_config, semaphore))

        responses = await asyncio.gather(*tasks)

        for id_value, response_data in zip(ids_list, responses):
            if response_data is None:
                continue

            if api_config.get("is_csv"):
                all_csv_texts.append(response_data)
            else:
                try:
                    # Normalização profunda: entra em '_embedded.contratacoes'
                    df_temp = pd.json_normalize(
                        response_data,
                        record_path=api_config['record_path'],
                        meta=api_config['meta'],
                        errors='ignore' # Ignora chaves que podem não existir em todos os objetos
                    )
                    if not df_temp.empty:
                        all_dataframes.append(df_temp)
                        logger.info(f"Dados do ID {id_value} processados com sucesso. {len(df_temp)} registros.")
                    else:
                        logger.warning(f"Nenhum dado encontrado para ID {id_value} na estrutura esperada de {api_config['record_path']}.")

                except Exception as e:
                    logger.error(f"Erro ao normalizar dados JSON para ID {id_value}: {e}. Dados brutos: {response_data}")

    # Processamento e salvamento final
    if api_config.get("is_csv"):
        if all_csv_texts:
            # Junta todos os CSVs. Pega o cabeçalho do primeiro e os dados dos demais.
            header = all_csv_texts[0].split('\n')[0]
            combined_csv_body = [header] + [csv_text.split('\n', 1)[1] for csv_text in all_csv_texts]
            final_csv_content = '\n'.join(combined_csv_body)
            # Remove linhas vazias se houver
            final_csv_content = '\n'.join(line for line in final_csv_content.splitlines() if line.strip())

            with open(output_filename, 'w', encoding='utf-8-sig') as f:
                f.write(final_csv_content)
            logger.info(f"Dados CSV combinados salvos em '{output_filename}'")
        else:
            logger.warning("Nenhum dado CSV foi coletado.")
    else:
        if all_dataframes:
            df_final = pd.concat(all_dataframes, ignore_index=True)

            # Formata colunas monetárias
            df_final = format_monetary_columns(df_final)

            # Remove colunas indesejadas que podem vir do metadata da API e não são úteis no CSV
            columns_to_drop = [col for col in ['_links.self.href', '_links.next.href', '_links.last.href'] if col in df_final.columns]
            if columns_to_drop:
                df_final = df_final.drop(columns=columns_to_drop)

            # Reordena colunas para colocar os IDs e nomes no início, se existirem
            preferred_order_start = [
                'idCompra', 'idContratacao', 'identificadorPNCP', 'numeroContrato',
                'nomeContratado', 'cnpjContratado', 'cpfContratado',
                'objeto', 'nomeOrgao', 'codigoOrgao'
            ]
            current_columns = df_final.columns.tolist()
            new_order = [col for col in preferred_order_start if col in current_columns and col not in columns_to_drop]
            remaining_columns = [col for col in current_columns if col not in new_order and col not in columns_to_drop]
            df_final = df_final[new_order + remaining_columns]


            df_final.to_csv(output_filename, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"Dados JSON normalizados e formatados salvos em '{output_filename}'")
        else:
            logger.warning("Nenhum DataFrame foi gerado a partir dos dados JSON.")

async def main():
    parser = argparse.ArgumentParser(description="Coletar dados do PNCP para uma lista de IDs.")
    parser.add_argument("--api", required=True, choices=list(API_ENDPOINTS.keys()),
                        help="Tipo de API a ser consultada (CONTRATACAO, ITENS, RESULTADOS, PRECOS_MATERIAL, PRECOS_SERVICO)")
    parser.add_argument("--output", required=True, help="Nome do arquivo de saída (ex: dados.csv)")
    args = parser.parse_args()

    ids = read_ids_from_file()
    if not ids:
        logger.error("Nenhum ID válido encontrado para processamento. Encerrando.")
        return

    logger.info(f"Iniciando coleta para a API: {args.api}")
    await collect_data(args.api, args.output, ids)
    logger.info("Coleta de dados concluída.")

if __name__ == "__main__":
    # Garante que o loop de eventos seja fechado corretamente
    try:
        asyncio.run(main())
    except RuntimeError as e:
        logger.critical(f"Erro de execução: {e}. Isso pode ocorrer se o loop de eventos estiver sendo gerenciado incorretamente. Certifique-se de que o programa não está tentando criar um novo loop ou usar um loop já fechado.")
    except KeyboardInterrupt:
        logger.info("Processo interrompido pelo usuário.")
    except Exception as e:
        logger.critical(f"Erro inesperado no programa principal: {e}", exc_info=True)
