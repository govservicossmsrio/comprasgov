import asyncio
import aiohttp
import pandas as pd
import argparse
import logging
import os
import io

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Limite de requisições concorrentes para evitar sobrecarregar a API
MAX_CONCURRENT_REQUESTS = 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Mapeamento de endpoints da API
# is_json: True para APIs que retornam JSON, False para CSV
# param_name: Nome do parâmetro de busca (ex: 'codigo' ou 'codigoItemCatalogo')
# record_path: Caminho para os dados reais dentro do JSON, se estiverem aninhados (usado por json_normalize)
# meta_fields: Campos de nível superior no JSON a serem incluídos em cada linha (usado por json_normalize)
API_ENDPOINTS = {
    "CONTRATACAO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "is_json": True,
        "record_path": ['_embedded', 'contratacoes'], # Caminho sugerido para dados aninhados
        "meta_fields": ['total', 'pagina', 'tamanhoPagina']
    },
    "ITENS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "is_json": True,
        "record_path": ['_embedded', 'itens'], # Assumindo 'itens' para esta API
        "meta_fields": ['total', 'pagina', 'tamanhoPagina']
    },
    "RESULTADOS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "is_json": True,
        "record_path": ['_embedded', 'resultados'], # Assumindo 'resultados' para esta API
        "meta_fields": ['total', 'pagina', 'tamanhoPagina']
    },
    "PRECOS_MATERIAL": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV",
        "param_name": "codigoItemCatalogo",
        "is_json": False,
        "additional_params": "&pagina=1&tamanhoPagina=1000" # Parâmetros adicionais para CSV
    },
    "PRECOS_SERVICO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV",
        "param_name": "codigoItemCatalogo",
        "is_json": False,
        "additional_params": "&pagina=1&tamanhoPagina=1000"
    }
}

# Colunas conhecidas que podem conter valores monetários e precisam de formatação
CURRENCY_COLUMNS = [
    'valorEstimado', 'valorTotal', 'valorUnitarioEstimado',
    'valorTotalLicitado', 'valorUnitario', 'precoUnitario',
    'valorCotado'
]

def read_ids_from_file(filename="id_lista.txt"):
    """Lê os IDs de um arquivo de texto, um por linha."""
    ids = []
    if not os.path.exists(filename):
        logger.error(f"Erro: O arquivo '{filename}' não foi encontrado na mesma pasta. Crie-o com um ID por linha.")
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'): # Ignora linhas vazias e comentários
                    ids.append(line)
        logger.info(f"Lidos {len(ids)} IDs do arquivo '{filename}'.")
        return ids
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo '{filename}': {e}")
        return None

async def fetch(session, url, api_type_config):
    """Faz uma requisição HTTP GET para a URL especificada."""
    async with semaphore: # Limita requisições concorrentes
        try:
            headers = {'accept': '*/*'}
            if not api_type_config['is_json']:
                headers = {'accept': 'text/csv'} # Para APIs que retornam CSV

            async with session.get(url, headers=headers, timeout=30) as response:
                response.raise_for_status()  # Levanta HTTPStatusError para respostas 4xx/5xx
                if api_type_config['is_json']:
                    return await response.json()
                else:
                    return await response.text() # Para conteúdo CSV
        except aiohttp.ClientTimeout:
            logger.warning(f"Timeout ao buscar {url}")
            return None
        except aiohttp.ClientError as e:
            logger.warning(f"Erro de rede/cliente ao buscar {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar {url}: {e}")
            return None

async def collect_data(api_type: str, ids: list):
    """
    Coleta dados de uma API específica para uma lista de IDs.
    Retorna uma lista de DataFrames (para JSON) ou strings CSV (para CSV).
    """
    api_config = API_ENDPOINTS.get(api_type)
    if not api_config:
        logger.error(f"Tipo de API '{api_type}' inválido. Escolha entre: {', '.join(API_ENDPOINTS.keys())}")
        return []

    logger.info(f"Iniciando coleta de dados para a API: {api_type}")
    base_url = api_config['url_base']
    param_name = api_config['param_name']
    is_json = api_config['is_json']
    additional_params = api_config.get('additional_params', '')

    all_results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for id_value in ids:
            if is_json:
                url = f"{base_url}?tipo={param_name}&codigo={id_value}"
            else:
                url = f"{base_url}?{param_name}={id_value}{additional_params}"
            tasks.append(fetch(session, url, api_config))

        responses = await asyncio.gather(*tasks)

        for i, response_data in enumerate(responses):
            id_value = ids[i] # O ID correspondente a esta resposta
            if response_data is None:
                logger.warning(f"Nenhum dado retornado para ID: {id_value} da API {api_type}. Pulando.")
                continue

            if is_json:
                df_current_id = pd.DataFrame()
                
                if isinstance(response_data, dict):
                    record_path = api_config.get('record_path')
                    meta_fields = api_config.get('meta_fields', [])
                    
                    # Tenta normalizar com record_path se ele existir no dicionário
                    if record_path and len(record_path) > 0:
                        try:
                            # Tenta com o record_path especificado
                            # json_normalize pode falhar se o path não existir ou não for uma lista/dict
                            nested_data = response_data
                            for key in record_path:
                                nested_data = nested_data[key]
                            
                            df_current_id = pd.json_normalize(nested_data, meta=meta_fields)
                        except KeyError:
                            logger.debug(f"Record_path '{record_path}' não encontrado ou inválido para ID {id_value}. Tentando normalizar o objeto raiz.")
                            # Fallback: tentar normalizar o dict raiz diretamente
                            df_current_id = pd.json_normalize(response_data)
                        except Exception as e:
                            logger.warning(f"Erro na normalização JSON com record_path {record_path} para ID {id_value}: {e}. Tentando normalizar o objeto raiz.")
                            df_current_id = pd.json_normalize(response_data)
                    else:
                        # Se não há record_path ou ele está vazio, normaliza o dict raiz
                        df_current_id = pd.json_normalize(response_data)

                elif isinstance(response_data, list):
                    # Se a resposta é diretamente uma lista de registros
                    df_current_id = pd.json_normalize(response_data)
                
                else:
                    logger.warning(f"Resposta JSON inesperada para ID: {id_value} da API {api_type}: {type(response_data)}. Esperado dict ou list.")
                    continue

                if not df_current_id.empty:
                    # Adiciona o ID original para rastreabilidade
                    df_current_id[f'id_{param_name}_original'] = id_value
                    all_results.append(df_current_id)
                else:
                    logger.info(f"Normalização JSON não retornou dados para ID: {id_value} da API {api_type} após tentar diferentes abordagens.")
            else: # Processamento para API que retorna CSV
                if response_data.strip(): # Verifica se o conteúdo não é vazio
                    all_results.append(response_data)
                else:
                    logger.warning(f"Resposta CSV vazia para ID: {id_value} da API {api_type}.")
    return all_results

def format_currency_column(df_col):
    """Formata uma coluna do DataFrame como moeda brasileira com 4 casas decimais."""
    # Converte para numérico, forçando NaNs para valores inválidos
    # Substitui vírgulas por pontos antes de converter para float para que o Pandas entenda
    numeric_col = pd.to_numeric(
        df_col.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
        errors='coerce'
    )
    # Aplica formatação e troca ponto por vírgula para o padrão brasileiro
    formatted_col = numeric_col.apply(lambda x: f'{x:.4f}'.replace('.', ',') if pd.notna(x) else '')
    return formatted_col

async def main():
    parser = argparse.ArgumentParser(description="Coleta dados do PNCP de forma assíncrona e salva em CSV.")
    parser.add_argument('--api', type=str, required=True,
                        choices=API_ENDPOINTS.keys(),
                        help="Tipo de API para consultar.")
    parser.add_argument('--output', type=str, required=True,
                        help="Nome do arquivo de saída CSV (ex: dados.csv).")
    args = parser.parse_args()

    # Leitura dos IDs do arquivo
    ids_to_process = read_ids_from_file()
    if not ids_to_process:
        logger.error("Nenhum ID válido para processar. Encerrando.")
        return

    # Coleta dos dados
    results = await collect_data(args.api, ids_to_process)

    if not results:
        logger.info("Nenhum dado coletado para salvar.")
        return

    api_config = API_ENDPOINTS[args.api]

    if api_config['is_json']:
        # Concatena todos os DataFrames JSON
        try:
            df_final = pd.concat(results, ignore_index=True)
            logger.info(f"Dados JSON compilados em um DataFrame com {len(df_final)} linhas e {len(df_final.columns)} colunas.")
        except ValueError as e:
            logger.error(f"Erro ao concatenar DataFrames JSON: {e}. Verifique se todos os DataFrames têm estruturas compatíveis.")
            logger.info("Tentando concatenar de forma mais flexível.")
            
            # Fallback para concatenação flexível, caso as colunas variem muito entre os resultados
            df_final = pd.DataFrame()
            if results:
                # Inicializa com o primeiro DataFrame para pegar um conjunto inicial de colunas
                df_final = results[0]
                for i in range(1, len(results)):
                    df_final = pd.concat([df_final, results[i]], ignore_index=True, sort=False)
            
            if df_final.empty:
                logger.error("DataFrame final JSON vazio após tentativas de concatenação. Encerrando.")
                return
        
        # Formatação de colunas de moeda
        for col in CURRENCY_COLUMNS:
            if col in df_final.columns:
                logger.info(f"Formatando coluna '{col}' como moeda.")
                df_final[col] = format_currency_column(df_final[col])
            else:
                logger.debug(f"Coluna '{col}' não encontrada no DataFrame final.")

        # Salva o DataFrame final em CSV com separador ';' e encoding utf-8-sig para Excel
        try:
            df_final.to_csv(args.output, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"Dados JSON salvos com sucesso em '{args.output}'.")
        except Exception as e:
            logger.error(f"Erro ao salvar o arquivo CSV para dados JSON: {e}")

    else: # Processamento para APIs que retornam CSV diretamente
        # Concatena os conteúdos CSV, garantindo que o cabeçalho seja escrito apenas uma vez
        full_csv_content = []
        header_written = False
        for csv_text in results:
            lines = csv_text.strip().split('\n')
            if not lines:
                continue

            if not header_written:
                full_csv_content.append(lines[0]) # Adiciona o cabeçalho do primeiro CSV
                header_written = True
                full_csv_content.extend(lines[1:]) # Adiciona os dados
            else:
                full_csv_content.extend(lines[1:]) # Adiciona os dados, pulando cabeçalhos subsequentes

        if not full_csv_content:
            logger.info("Nenhum conteúdo CSV para salvar.")
            return

        try:
            # Salva o conteúdo CSV bruto
            with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
                f.write('\n'.join(full_csv_content))
            logger.info(f"Dados CSV salvos com sucesso em '{args.output}'.")
        except Exception as e:
            logger.error(f"Erro ao salvar o arquivo CSV para dados diretos de CSV: {e}")

if __name__ == "__main__":
    asyncio.run(main())
