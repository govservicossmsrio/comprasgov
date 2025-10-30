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
        "url": "https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "id_param": "codigo",
        "is_csv": False,
        "accept_header": "application/json"
    },
    "ITENS": {
        "url": "https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "id_param": "codigo",
        "is_csv": False,
        "accept_header": "application/json"
    },
    "RESULTADOS": {
        "url": "https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id",
        "id_param": "codigo",
        "is_csv": False,
        "accept_header": "application/json"
    },
    "PRECOS_MATERIAL": {
        "url": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV",
        "id_param": "codigoItemCatalogo",
        "is_csv": True,
        "accept_header": "text/csv"
    },
    "PRECOS_SERVICO": {
        "url": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV",
        "id_param": "codigoItemCatalogo",
        "is_csv": True,
        "accept_header": "text/csv"
    },
}

async def fetch_data(session, api_config, item_id, semaphore):
    """
    Faz uma requisição assíncrona para a API e retorna os dados.
    """
    url_base = api_config["url"]
    id_param = api_config["id_param"]
    is_csv = api_config["is_csv"]
    accept_header = api_config["accept_header"]

    # Construir a URL com os parâmetros corretos
    if is_csv:
        # APIs de preço CSV usam 'pagina' e 'tamanhoPagina' e o id_param
        # Aqui assumimos pagina=1 e tamanhoPagina=10, mas poderiam ser configuráveis.
        # O id_param aqui é codigoItemCatalogo
        url = f"{url_base}?pagina=1&tamanhoPagina=10&{id_param}={item_id}"
    else:
        # APIs de contratação usam 'tipo=idCompra' e o id_param
        # O id_param aqui é 'codigo'
        url = f"{url_base}?tipo=idCompra&{id_param}={item_id}"
    
    headers = {'accept': accept_header}

    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                response.raise_for_status()  # Levanta HTTPError para códigos de status 4xx/5xx

                if is_csv:
                    return await response.text()  # Retorna texto puro para CSV
                else:
                    return await response.json() # Retorna JSON para as outras APIs
        except aiohttp.ClientError as e:
            logger.error(f"Erro ao buscar dados para ID {item_id} da API {api_config['url']}: {e}")
            return None
        except asyncio.TimeoutError:
            logger.warning(f"Timeout ao buscar dados para ID {item_id} da API {api_config['url']}")
            return None

async def main(api_name, output_filename):
    """
    Função principal que coordena a busca e o salvamento dos dados.
    """
    if api_name not in API_ENDPOINTS:
        logger.error(f"Tipo de API '{api_name}' inválido. Opções válidas são: {', '.join(API_ENDPOINTS.keys())}")
        return

    api_config = API_ENDPOINTS[api_name]
    logger.info(f"Iniciando coleta de dados para a API: {api_name}")

    ids = read_ids_from_file("id_lista.txt")
    if not ids:
        logger.error("Nenhum ID válido encontrado em 'id_lista.txt'. Encerrando.")
        return

    all_data = []
    # Limita o número de requisições concorrentes
    semaphore = asyncio.Semaphore(3) 

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, api_config, item_id, semaphore) for item_id in ids]
        results = await asyncio.gather(*tasks)

        if api_config["is_csv"]:
            # Processamento para APIs que retornam CSV diretamente
            header_written = False
            with open(output_filename, 'w', encoding='utf-8') as f:
                for idx, result in enumerate(results):
                    if result:
                        lines = result.strip().split('\n')
                        if not header_written:
                            f.write(lines[0] + '\n') # Escreve o cabeçalho apenas uma vez
                            header_written = True
                        if len(lines) > 1:
                            f.write('\n'.join(lines[1:]) + '\n') # Escreve as linhas de dados (sem o cabeçalho)
                        logger.info(f"Dados CSV para ID {ids[idx]} processados com sucesso.")
                    else:
                        logger.warning(f"Nenhum dado CSV retornado para ID {ids[idx]}.")
            logger.info(f"Todos os dados CSV compilados em '{output_filename}'.")
        else:
            # Processamento para APIs que retornam JSON
            for idx, result in enumerate(results):
                if result:
                    # A API pode retornar uma lista de objetos JSON ou um único objeto
                    if isinstance(result, list):
                        all_data.extend(result)
                    else:
                        all_data.append(result)
                    logger.info(f"Dados JSON para ID {ids[idx]} processados com sucesso.")
                else:
                    logger.warning(f"Nenhum dado JSON retornado para ID {ids[idx]}.")
            
            if all_data:
                df_final = pd.DataFrame(all_data)
                df_final.to_csv(output_filename, index=False, encoding='utf-8', sep=';')
                logger.info(f"Todos os dados JSON compilados e salvos em '{output_filename}'.")
            else:
                logger.warning("Nenhum dado JSON foi coletado. Nenhum arquivo CSV será gerado.")


def read_ids_from_file(filename):
    """
    Lê uma lista de IDs de um arquivo de texto.
    Cada ID deve estar em uma nova linha.
    """
    ids = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Ignora linhas vazias
                    ids.append(line)
        logger.info(f"{len(ids)} IDs lidos de '{filename}'.")
    except FileNotFoundError:
        logger.error(f"Erro: O arquivo '{filename}' não foi encontrado no diretório atual.")
    except Exception as e:
        logger.error(f"Erro ao ler IDs do arquivo '{filename}': {e}")
    return ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coleta dados da API Compras.gov.br PNCP.")
    parser.add_argument(
        "--api",
        type=str,
        required=True,
        choices=API_ENDPOINTS.keys(),
        help="Tipo de API a ser consultada."
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Nome do arquivo de saída (ex: dados.csv)."
    )

    args = parser.parse_args()

    asyncio.run(main(args.api, args.output))
