## coletar_dados_pncp.py
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

# Mapeamento de endpoints da API
API_ENDPOINTS = {
    "CONTRATACAO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "query_type_param": "idCompra", # Parâmetro 'tipo' específico para esta família de APIs
        "param_name": "codigo",        # Nome do parâmetro que recebe o ID
        "is_json": True                # Indica que a resposta esperada é JSON
    },
    "ITENS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "query_type_param": "idCompra",
        "param_name": "codigo",
        "is_json": True
    },
    "RESULTADOS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id",
        "query_type_param": "idCompra",
        "param_name": "codigo",
        "is_json": True
    },
    "PRECOS_MATERIAL": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV",
        "param_name": "codigoItemCatalogo", # Sem parâmetro 'tipo', o ID vai direto aqui
        "is_json": False,                   # Indica que a resposta esperada é CSV
        "csv_params": "&pagina=1&tamanhoPagina=10" # Parâmetros extras para APIs CSV
    },
    "PRECOS_SERVICO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV",
        "param_name": "codigoItemCatalogo",
        "is_json": False,
        "csv_params": "&pagina=1&tamanhoPagina=10"
    }
}

async def fetch_data(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore, is_json: bool, id_param_name: str) -> dict:
    """
    Faz uma requisição HTTP assíncrona para a URL especificada.
    Retorna o JSON da resposta ou o texto bruto se for CSV.
    Inclui o ID da requisição para facilitar o logging de erros.
    """
    async with semaphore:
        current_id = "N/A"  # Valor padrão para o ID em caso de falha na análise da URL
        try:
            # Tenta extrair o ID para logs e relatórios de erro
            if f"{id_param_name}=" in url:
                current_id = url.split(f'{id_param_name}=')[1].split('&')[0]

            async with session.get(url, headers={'accept': '*/*'}) as response:
                response.raise_for_status()  # Levanta HTTPError para respostas 4xx/5xx
                if is_json:
                    return {'status': 'success', 'id': current_id, 'data': await response.json()}
                else:
                    return {'status': 'success', 'id': current_id, 'data': await response.text()}
        except aiohttp.ClientError as e:
            logger.error(f"Erro de rede ou cliente ao buscar {url} (ID: {current_id}): {e}")
            return {'status': 'error', 'id': current_id, 'error': str(e)}
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar {url} (ID: {current_id}): {e}")
            return {'status': 'error', 'id': current_id, 'error': str(e)}

def read_ids_from_file(filename: str) -> list:
    """
    Lê IDs de um arquivo de texto, um ID por linha.
    """
    ids = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_id = line.strip()
                if stripped_id:
                    ids.append(stripped_id)
        logger.info(f"Lidos {len(ids)} IDs do arquivo '{filename}'.")
    except FileNotFoundError:
        logger.error(f"Erro: O arquivo '{filename}' não foi encontrado. Por favor, crie-o com os IDs desejados (um por linha).")
        exit(1)
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo '{filename}': {e}")
        exit(1)
    return ids

async def main():
    parser = argparse.ArgumentParser(description="Coletar dados de contratações do PNCP via API.")
    parser.add_argument('--api', type=str, required=True, choices=API_ENDPOINTS.keys(),
                        help="Tipo de API para consultar (e.g., CONTRATACAO, ITENS, RESULTADOS, PRECOS_MATERIAL, PRECOS_SERVICO)")
    parser.add_argument('--output', type=str, required=True,
                        help="Nome do arquivo de saída (e.g., dados.csv)")
    args = parser.parse_args()

    api_config = API_ENDPOINTS[args.api]
    url_base = api_config["url_base"]
    param_name = api_config["param_name"]
    is_json_api = api_config["is_json"]
    csv_extra_params = api_config.get("csv_params", "")
    query_type_param = api_config.get("query_type_param") # Será None para APIs CSV

    id_list_filename = "id_lista.txt"
    ids = read_ids_from_file(id_list_filename)

    if not ids:
        logger.warning("Nenhum ID encontrado em 'id_lista.txt'. Encerrando.")
        return

    # Limita o número de requisições concorrentes para evitar sobrecarga na API
    MAX_CONCURRENT_REQUESTS = 5
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    tasks = []
    async with aiohttp.ClientSession() as session:
        for item_id in ids:
            if is_json_api:
                # Para APIs JSON, o formato da URL é: ...?tipo=idCompra&codigo=ID
                url = f"{url_base}?tipo={query_type_param}&{param_name}={item_id}"
            else:
                # Para APIs CSV, o formato é: ...?codigoItemCatalogo=ID&pagina=1&tamanhoPagina=10
                url = f"{url_base}?{param_name}={item_id}{csv_extra_params}"
            tasks.append(fetch_data(session, url, semaphore, is_json_api, param_name))

        logger.info(f"Iniciando a busca de dados para {len(ids)} IDs usando a API: {args.api}")
        responses = await asyncio.gather(*tasks)

    # Processar as respostas de acordo com o tipo de API
    if is_json_api:
        all_dfs = []
        for response in responses:
            if response['status'] == 'success' and response['data']:
                try:
                    # Usa json_normalize para achatar e criar DataFrame, lidando com objetos e listas de objetos
                    df = pd.json_normalize(response['data'])
                    if not df.empty:
                        all_dfs.append(df)
                    else:
                        logger.info(f"Dados vazios após normalização para ID {response.get('id', 'desconhecido')}.")
                except Exception as e:
                    logger.error(f"Erro ao normalizar JSON para ID {response['id']}: {e}")
            elif response['status'] == 'error':
                logger.warning(f"Falha ao buscar dados para ID {response['id']}: {response.get('error', 'Erro desconhecido')}")
            else:
                logger.info(f"Nenhum dado retornado para ID {response.get('id', 'desconhecido')}.")

        if all_dfs:
            df_final = pd.concat(all_dfs, ignore_index=True)
            output_filename = args.output
            # Salvar em CSV usando ponto e vírgula como separador para compatibilidade com Excel
            df_final.to_csv(output_filename, index=False, encoding='utf-8', sep=';')
            logger.info(f"Dados compilados salvos em '{output_filename}' ({len(df_final)} linhas).")
        else:
            logger.warning("Nenhum dado JSON válido foi coletado para salvar.")
    else: # Lógica para APIs que retornam CSV puro
        all_csv_lines = []
        header_written = False
        for response in responses:
            if response['status'] == 'success' and response['data']:
                try:
                    csv_content = response['data']
                    csv_io = io.StringIO(csv_content)
                    
                    # Usa pd.read_csv para parsear o conteúdo, assumindo que a API CSV usa vírgula como separador
                    df = pd.read_csv(csv_io, sep=',', encoding='utf-8') 
                    
                    if not df.empty:
                        if not header_written:
                            # Pega o cabeçalho do primeiro CSV e adiciona na lista
                            all_csv_lines.append(','.join(df.columns) + '\n')
                            header_written = True
                        
                        # Adiciona as linhas de dados (sem o cabeçalho)
                        for _, row in df.iterrows():
                            # Converte todos os valores para string antes de juntar para evitar erros de tipo
                            all_csv_lines.append(','.join(row.astype(str)) + '\n')
                    else:
                        logger.info(f"Resposta CSV vazia para ID {response.get('id', 'desconhecido')}. Ignorando.")
                        
                except pd.errors.EmptyDataError:
                    logger.warning(f"Resposta CSV vazia ou inválida para ID {response['id']}. Ignorando.")
                except Exception as e:
                    logger.error(f"Erro ao processar CSV para ID {response['id']}: {e}")
            elif response['status'] == 'error':
                logger.warning(f"Falha ao buscar dados para ID {response['id']}: {response.get('error', 'Erro desconhecido')}")
            else:
                logger.info(f"Nenhum dado CSV retornado para ID {response.get('id', 'desconhecido')}.")

        if all_csv_lines:
            output_filename = args.output
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.writelines(all_csv_lines)
            logger.info(f"Dados CSV compilados salvos em '{output_filename}' ({len(all_csv_lines)-1} linhas de dados).")
        else:
            logger.warning("Nenhum dado CSV válido foi coletado para salvar.")

if __name__ == "__main__":
    asyncio.run(main())
