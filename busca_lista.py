import asyncio
import aiohttp
import pandas as pd
import argparse
import logging
import os
import io

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Mapeamento de Endpoints de API ---
API_ENDPOINTS = {
    "CONTRATACAO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "is_json": True,
        "default_filename": "contratacoes.csv"
    },
    "ITENS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "is_json": True,
        "default_filename": "itens_contratacoes.csv"
    },
    "RESULTADOS": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id",
        "param_name": "codigo",
        "is_json": True,
        "default_filename": "resultados_itens.csv"
    },
    "PRECOS_MATERIAL": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV",
        "param_name": "codigoItemCatalogo",
        "is_json": False,
        "default_filename": "precos_material.csv"
    },
    "PRECOS_SERVICO": {
        "url_base": "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV",
        "param_name": "codigoItemCatalogo",
        "is_json": False,
        "default_filename": "precos_servico.csv"
    }
}

async def fetch(session, url, current_id, api_type):
    """
    Realiza uma requisição HTTP GET assíncrona para a URL especificada.
    Retorna o conteúdo da resposta se bem-sucedida, caso contrário, None.
    """
    try:
        async with session.get(url, headers={'accept': '*/*'}) as response:
            response.raise_for_status()  # Lança exceção para códigos de status HTTP 4xx/5xx
            if api_type["is_json"]:
                return await response.json()
            else:
                return await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"Erro ao buscar {current_id} da API {api_type['url_base']}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao buscar {current_id} da API {api_type['url_base']}: {e}")
        return None

async def bound_fetch(sem, session, url, current_id, api_type):
    """
    Limita a concorrência das requisições usando um semáforo.
    """
    async with sem:
        return await fetch(session, url, current_id, api_type)

async def collect_data(ids, api_info, max_concurrent_requests=3):
    """
    Coleta dados de múltiplas APIs de forma assíncrona.
    """
    all_results = []
    # Usado para concatenar CSVs, mantendo o cabeçalho apenas do primeiro
    csv_parts = []
    first_csv_header = None

    sem = asyncio.Semaphore(max_concurrent_requests)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for current_id in ids:
            url = f"{api_info['url_base']}?tipo=idCompra&{api_info['param_name']}={current_id}"
            if not api_info["is_json"]: # Para APIs de preço, adicionar paginação default
                if api_info["param_name"] == "codigoItemCatalogo":
                    # Ajusta a URL para incluir os parâmetros de paginação para as APIs de preço
                    url = f"{api_info['url_base']}?pagina=1&tamanhoPagina=10&{api_info['param_name']}={current_id}"
                else: # Mantém o tipo=idCompra para outros casos se houver
                    url = f"{api_info['url_base']}?tipo=idCompra&{api_info['param_name']}={current_id}"

            tasks.append(bound_fetch(sem, session, url, current_id, api_info))

        logging.info(f"Iniciando a coleta de dados para {len(ids)} IDs...")
        responses = await asyncio.gather(*tasks)
        logging.info("Coleta de dados concluída.")

        for i, data in enumerate(responses):
            current_id = ids[i]
            if data:
                if api_info["is_json"]:
                    if isinstance(data, list):
                        all_results.extend(data)
                    else:
                        all_results.append(data)
                else: # Processa como CSV
                    # Divide o CSV em linhas
                    lines = data.strip().split('\n')
                    if not lines:
                        continue

                    # Se for o primeiro CSV, armazena o cabeçalho
                    if first_csv_header is None:
                        first_csv_header = lines[0]
                        csv_parts.append(first_csv_header)
                        # Adiciona o restante das linhas (sem o cabeçalho)
                        csv_parts.extend(lines[1:])
                    else:
                        # Para os CSVs subsequentes, adiciona apenas os dados (ignora o cabeçalho)
                        if len(lines) > 1: # Garante que há dados além do cabeçalho
                            csv_parts.extend(lines[1:])
            else:
                logging.warning(f"Nenhum dado retornado para o ID: {current_id}")

    if api_info["is_json"]:
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
    else: # Retorna o conteúdo CSV concatenado como uma única string
        return '\n'.join(csv_parts) if csv_parts else ""

def read_ids_from_file(filepath="id_lista.txt"):
    """
    Lê IDs de um arquivo de texto, um por linha.
    Retorna uma lista de IDs.
    """
    ids = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line:  # Ignora linhas vazias
                    ids.append(stripped_line)
        logging.info(f"Lidos {len(ids)} IDs do arquivo '{filepath}'.")
    except FileNotFoundError:
        logging.error(f"Erro: O arquivo '{filepath}' não foi encontrado. Certifique-se de que ele existe e está na mesma pasta.")
        exit(1)
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo '{filepath}': {e}")
        exit(1)
    return ids

def main():
    parser = argparse.ArgumentParser(description="Coleta dados do PNCP (Painel Nacional de Contratações Públicas) de forma assíncrona.")
    parser.add_argument(
        "--api",
        type=str,
        required=True,
        choices=API_ENDPOINTS.keys(),
        help="Selecione o tipo de API a ser consultada: " + ", ".join(API_ENDPOINTS.keys())
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Nome do arquivo CSV de saída. Ex: dados.csv"
    )
    parser.add_argument(
        "--ids_file",
        type=str,
        default="id_lista.txt",
        help="Caminho para o arquivo contendo a lista de IDs, um por linha."
    )

    args = parser.parse_args()

    # Valida a API selecionada
    api_info = API_ENDPOINTS.get(args.api)
    if not api_info:
        logging.error(f"API '{args.api}' inválida. Escolha entre: {', '.join(API_ENDPOINTS.keys())}")
        return

    # Define o nome do arquivo de saída
    output_filename = args.output if args.output else api_info["default_filename"]
    if not output_filename.endswith('.csv'):
        output_filename += '.csv'

    logging.info(f"API selecionada: {args.api}")
    logging.info(f"Arquivo de IDs: {args.ids_file}")
    logging.info(f"Arquivo de saída: {output_filename}")

    # Lê os IDs do arquivo
    ids_to_fetch = read_ids_from_file(args.ids_file)
    if not ids_to_fetch:
        logging.warning("Nenhum ID encontrado no arquivo. Encerrando.")
        return

    # Executa a coleta de dados assíncrona
    try:
        if api_info["is_json"]:
            df = asyncio.run(collect_data(ids_to_fetch, api_info))
            if not df.empty:
                df.to_csv(output_filename, index=False, encoding='utf-8')
                logging.info(f"Dados compilados salvos em '{output_filename}' (formato JSON -> CSV).")
            else:
                logging.warning("Nenhum dado foi coletado para salvar.")
        else: # É uma API que retorna CSV diretamente
            csv_content = asyncio.run(collect_data(ids_to_fetch, api_info))
            if csv_content:
                with open(output_filename, 'w', encoding='utf-8') as f:
                    f.write(csv_content)
                logging.info(f"Dados compilados salvos em '{output_filename}' (formato CSV direto).")
            else:
                logging.warning("Nenhum dado CSV foi coletado para salvar.")
    except Exception as e:
        logging.critical(f"Ocorreu um erro fatal durante a execução: {e}")

if __name__ == '__main__':
    main()
