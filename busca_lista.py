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

# Limite de requisições concorrentes
MAX_CONCURRENT_REQUESTS = 3

# Mapeamento de endpoints da API
API_ENDPOINTS = {
    'CONTRATACAO': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id',
        'param': 'codigo',
        'is_json': True
    },
    'ITENS': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id',
        'param': 'codigo',
        'is_json': True
    },
    'RESULTADOS': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id',
        'param': 'codigo',
        'is_json': True
    },
    'PRECOS_MATERIAL': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV',
        'param': 'codigoItemCatalogo',
        'is_json': False
    },
    'PRECOS_SERVICO': {
        'url_base': 'https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV',
        'param': 'codigoItemCatalogo',
        'is_json': False
    }
}

async def fetch(session, url, item_id, semaphore):
    """
    Realiza uma requisição HTTP GET assíncrona.
    """
    async with semaphore:  # Limita requisições concorrentes
        try:
            logger.info(f"Iniciando requisição para ID: {item_id} na URL: {url}")
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()  # Lança exceção para status HTTP de erro
                if response.content_type == 'application/json':
                    return item_id, await response.json()
                else: # Assume CSV ou texto plano
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
    Ignora linhas vazias ou inválidas.
    """
    ids = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                item_id = line.strip()
                if item_id:  # Ignora linhas em branco
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
    Formata colunas que provavelmente contêm valores monetários para o padrão brasileiro
    com 4 casas decimais e vírgula como separador decimal.
    Converte para numérico antes de formatar e trata valores nulos.
    """
    money_cols_patterns = [
        'valorEstimado', 'valorTotal', 'valorUnitarioEstimado',
        'valorHomologado', 'valorUnitarioHomologado', 'valorTotalHomologado',
        'valorUnitarioResultado', 'valorTotalResultado', 'valorMensalEstimado',
        'valor', 'vlr'
    ]
    
    # Criar um regex que combine todos os padrões para busca de colunas
    money_cols_regex = re.compile(r'|'.join(money_cols_patterns), re.IGNORECASE)

    for col in df.columns:
        if money_cols_regex.search(col):
            # Converte a coluna para numérico, transformando erros em NaN
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
    param_name = endpoint_info['param']
    is_json = endpoint_info['is_json']

    all_dfs = []
    all_csv_texts = []
    header_written = False # Para APIs CSV, garante que o cabeçalho seja escrito apenas uma vez

    async with aiohttp.ClientSession() as session:
        tasks = []
        for item_id in ids_to_process:
            url = f"{url_base}?{param_name}={item_id}"
            if is_json:
                tasks.append(fetch(session, url, item_id, semaphore))
            else: # Para APIs que retornam CSV, o parâmetro 'tipo' é padrão 'codigoItemCatalogo'
                # A URL já está correta, apenas garante o nome do parâmetro
                tasks.append(fetch(session, url, item_id, semaphore))

        responses = await asyncio.gather(*tasks)

        for item_id, data in responses:
            if data is None:
                continue

            if is_json:
                if isinstance(data, dict):
                    # Tenta normalizar a chave 'resultado', que é o padrão observado
                    if 'resultado' in data and isinstance(data['resultado'], list):
                        try:
                            df_response = pd.json_normalize(data['resultado'])
                            all_dfs.append(df_response)
                        except Exception as e:
                            logger.error(f"Erro ao normalizar dados JSON para ID {item_id} (chave 'resultado'): {e}. Dados brutos (parcial): {str(data)[:500]}...")
                    # Fallback caso a chave 'resultado' não exista (embora os exemplos indiquem que sempre existirá para estas APIs)
                    elif 'resultado' not in data and len(data) > 0:
                        try:
                            df_response = pd.json_normalize(data)
                            all_dfs.append(df_response)
                        except Exception as e:
                            logger.error(f"Erro ao normalizar dados JSON para ID {item_id} (raiz): {e}. Dados brutos (parcial): {str(data)[:500]}...")
                    else:
                        logger.warning(f"Resposta JSON para ID {item_id} não contém 'resultado' ou está vazia. Dados brutos (parcial): {str(data)[:500]}...")
                elif isinstance(data, list) and len(data) > 0:
                    # Se a resposta é uma lista diretamente (ex: sem 'resultado' ou '_embedded')
                    try:
                        df_response = pd.json_normalize(data)
                        all_dfs.append(df_response)
                    except Exception as e:
                        logger.error(f"Erro ao normalizar dados JSON para ID {item_id} (lista direta): {e}. Dados brutos (parcial): {str(data)[:500]}...")
                else:
                    logger.warning(f"Resposta JSON para ID {item_id} não pôde ser processada como dicionário ou lista. Dados brutos: {data}")
            else: # Processar como CSV
                # Para APIs CSV, a resposta é o texto CSV completo
                if not header_written:
                    all_csv_texts.append(data)
                    header_written = True
                else:
                    # Remove o cabeçalho das strings CSV subsequentes
                    lines = data.splitlines()
                    if len(lines) > 1:
                        all_csv_texts.append("\n".join(lines[1:]))

    if is_json:
        if all_dfs:
            # Concatena todos os DataFrames, preenchendo com NaN para colunas ausentes
            final_df = pd.concat(all_dfs, ignore_index=True, join='outer')
            final_df = format_money_columns(final_df) # Formata as colunas monetárias
            return final_df
        else:
            logger.warning("Nenhum DataFrame foi gerado a partir dos dados JSON.")
            return None
    else: # Retornar texto CSV concatenado
        if all_csv_texts:
            return "\n".join(all_csv_texts)
        else:
            logger.warning("Nenhum texto CSV foi coletado.")
            return None

async def main():
    parser = argparse.ArgumentParser(description="Coleta dados do PNCP.")
    parser.add_argument('--api', type=str, required=True,
                        choices=['CONTRATACAO', 'ITENS', 'RESULTADOS', 'PRECOS_MATERIAL', 'PRECOS_SERVICO'],
                        help="Tipo de API a ser consultada.")
    parser.add_argument('--output', type=str, required=True,
                        help="Nome do arquivo de saída (ex: dados.csv).")
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
    
    # O semáforo deve ser criado no mesmo loop onde será usado (aqui, dentro de main)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    data_result = await collect_data(args.api, ids_to_process, semaphore)

    output_filename = args.output

    if data_result is not None:
        if isinstance(data_result, pd.DataFrame):
            try:
                # Usa 'utf-8-sig' para melhor compatibilidade com Excel e caracteres especiais
                # Usa ';' como separador para compatibilidade regional do Excel
                data_result.to_csv(output_filename, index=False, encoding='utf-8-sig', sep=';')
                logger.info(f"Dados coletados salvos em '{output_filename}' (formato DataFrame CSV).")
            except Exception as e:
                logger.error(f"Erro ao salvar DataFrame em '{output_filename}': {e}")
        elif isinstance(data_result, str): # Resultado de APIs CSV
            try:
                with open(output_filename, 'w', encoding='utf-8-sig') as f:
                    f.write(data_result)
                logger.info(f"Dados coletados salvos em '{output_filename}' (formato CSV bruto).")
            except Exception as e:
                logger.error(f"Erro ao salvar texto CSV em '{output_filename}': {e}")
    else:
        logger.warning("Nenhum dado foi coletado para salvar.")

    logger.info("Coleta de dados concluída.")

if __name__ == '__main__':
    asyncio.run(main())
