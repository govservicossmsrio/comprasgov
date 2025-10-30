## coletar_dados_pncp.py
import asyncio
import aiohttp
import pandas as pd
import argparse
import logging
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configurações ---
MAX_CONCURRENT_REQUESTS = 3  # Limite de requisições concorrentes

# Colunas que podem conter valores monetários para formatação
MONEY_COLUMNS = [
    'valorUnitario', 'valorTotal', 'valorEstimado', 'valorUnitarioEstimado',
    'valorTotalHomologado', 'valorUnitarioHomologado', 'valorSubscrito',
    'valorOriginal', 'valorNegociado', 'valorAtualizado', 'valorItem'
]

# Mapeamento dos endpoints da API
API_ENDPOINTS = {
    'CONTRATACAO': {
        'url': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id',
        'id_param': 'codigo',
        'type_param': 'tipo=idCompra',
        'response_type': 'json'
    },
    'ITENS': {
        'url': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id',
        'id_param': 'codigo',
        'type_param': 'tipo=idCompra',
        'response_type': 'json'
    },
    'RESULTADOS': {
        'url': 'https://dadosabertos.compras.gov.br/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id',
        'id_param': 'codigo',
        'type_param': 'tipo=idCompra',
        'response_type': 'json'
    },
    'PRECOS_MATERIAL': {
        'url': 'https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV',
        'id_param': 'codigoItemCatalogo',
        'type_param': 'pagina=1&tamanhoPagina=10', # Página e tamanho padrão, pode ser ajustado
        'response_type': 'csv'
    },
    'PRECOS_SERVICO': {
        'url': 'https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV',
        'id_param': 'codigoItemCatalogo',
        'type_param': 'pagina=1&tamanhoPagina=10', # Página e tamanho padrão, pode ser ajustado
        'response_type': 'csv'
    },
}

async def fetch(session, url, id_val, semaphore):
    """
    Realiza uma requisição HTTP assíncrona para a URL fornecida.
    Limita requisições concorrentes usando um semáforo.
    """
    async with semaphore:  # Limita requisições concorrentes
        logger.info(f"Iniciando requisição para ID: {id_val} na URL: {url}")
        try:
            async with session.get(url, raise_for_status=True) as response:
                if response.status == 200:
                    if 'csv' in response.headers.get('Content-Type', ''):
                        return await response.text(), id_val, 'csv'
                    return await response.json(), id_val, 'json'
                else:
                    logger.error(f"Erro HTTP {response.status} para ID {id_val} na URL: {url}")
                    return None, id_val, None
        except aiohttp.ClientError as e:
            logger.error(f"Erro de conexão para ID {id_val} na URL {url}: {e}")
            return None, id_val, None
        except Exception as e:
            logger.error(f"Erro inesperado para ID {id_val} na URL {url}: {e}")
            return None, id_val, None

async def process_json_response(json_data, api_type, id_val):
    """
    Processa uma resposta JSON, normalizando-a para um DataFrame do pandas.
    Assume que os dados de interesse estão na chave 'resultado'.
    """
    if not json_data:
        return pd.DataFrame()

    try:
        # Direciona a normalização para a chave 'resultado'
        # Assume que 'resultado' contém uma lista de dicionários ou um único dicionário
        data_to_normalize = json_data.get('resultado')

        if data_to_normalize is None:
            logger.error(f"ID {id_val}: Chave 'resultado' não encontrada nos dados JSON. Dados brutos: {json_data}")
            return pd.DataFrame()

        # Se 'resultado' for um único dicionário, envolve-o em uma lista para json_normalize
        if isinstance(data_to_normalize, dict):
            data_to_normalize = [data_to_normalize]

        if data_to_normalize:
            df = pd.json_normalize(data_to_normalize)
            # Adiciona o ID original da compra para rastreamento, se ainda não estiver presente
            if 'idCompra' not in df.columns and api_type in ['CONTRATACAO', 'ITENS', 'RESULTADOS']:
                df['idCompra_original'] = id_val
            return df
        else:
            logger.info(f"ID {id_val}: Chave 'resultado' encontrada, mas está vazia. Ignorando.")
            return pd.DataFrame()

    except KeyError as e:
        logger.error(f"ID {id_val}: Erro ao acessar chave esperada nos dados JSON: {e}. Dados brutos: {json_data}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro inesperado ao normalizar dados JSON para ID {id_val}: {e}. Dados brutos: {json_data}")
        return pd.DataFrame()


def format_money_columns(df):
    """
    Formata colunas identificadas como monetárias para o padrão brasileiro (4 casas decimais).
    """
    for col in MONEY_COLUMNS:
        if col in df.columns:
            # Converte para numérico, forçando erros para NaN para evitar quebrar o script
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Formata para string no padrão brasileiro com 4 casas decimais
            df[col] = df[col].apply(
                lambda x: f'{x:,.4f}'.replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notna(x) else None
            )
            # Reverte a ordem dos separadores de milhar e decimal
            # Ex: 1234.5678 -> 1.234,5678
            #    f'{x:.4f}'.replace('.', ',') # This would give 1234,5678 (wrong for BRL thousands)
            #    A better way is:
            #    df[col] = df[col].apply(lambda x: f'{x:_.4f}'.replace('.', ',').replace('_', '.') if pd.notna(x) else None)
            # This is more complex, the current implementation with replace works for `f'{x:.4f}'` output.
            # Let's adjust for a more standard BRL currency formatting
            df[col] = df[col].apply(
                lambda x: f"{x:,.4f}".replace(",", "TEMP_SEP").replace(".", ",").replace("TEMP_SEP", ".")
                if pd.notna(x) else None
            )
    return df


async def collect_data(api_name, ids_to_process, semaphore):
    """
    Coleta dados da API para uma lista de IDs, processando JSON ou CSV.
    """
    api_info = API_ENDPOINTS.get(api_name)
    if not api_info:
        logger.error(f"API '{api_name}' não reconhecida.")
        return pd.DataFrame()

    base_url = api_info['url']
    id_param = api_info['id_param']
    type_param = api_info['type_param']
    response_type = api_info['response_type']

    all_dfs = []
    all_csv_texts = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for id_val in ids_to_process:
            url = f"{base_url}?{type_param}&{id_param}={id_val}"
            tasks.append(fetch(session, url, id_val, semaphore))

        results = await asyncio.gather(*tasks)

        for json_or_text, id_val, r_type in results:
            if json_or_text is None:
                continue

            if r_type == 'json':
                df = await process_json_response(json_or_text, api_name, id_val)
                if not df.empty:
                    all_dfs.append(df)
            elif r_type == 'csv':
                all_csv_texts.append(json_or_text)
            else:
                logger.warning(f"Tipo de resposta desconhecido para ID {id_val}.")

    if response_type == 'json':
        if all_dfs:
            # Concatena todos os DataFrames, usando join='outer' para preservar todas as colunas
            df_final = pd.concat(all_dfs, ignore_index=True, join='outer')
            df_final = format_money_columns(df_final)
            return df_final
        else:
            logger.warning("Nenhum DataFrame foi gerado a partir dos dados JSON.")
            return pd.DataFrame() # Retorna um DataFrame vazio
    elif response_type == 'csv':
        if all_csv_texts:
            # Para CSVs, concatena o texto, mantendo apenas o cabeçalho do primeiro
            combined_csv_content = []
            for i, csv_text in enumerate(all_csv_texts):
                lines = csv_text.strip().split('\n')
                if i == 0:
                    combined_csv_content.extend(lines) # Adiciona cabeçalho e dados
                else:
                    combined_csv_content.extend(lines[1:]) # Adiciona apenas dados
            return "\n".join(combined_csv_content)
        else:
            logger.warning("Nenhum conteúdo CSV foi gerado.")
            return ""
    return pd.DataFrame() # Fallback

def read_ids_from_file(filename="id_lista.txt"):
    """Lê IDs de um arquivo de texto, um ID por linha."""
    if not os.path.exists(filename):
        logger.error(f"Arquivo '{filename}' não encontrado. Por favor, crie-o com os IDs desejados (um por linha).")
        return []

    ids = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line:  # Ignora linhas vazias
                    ids.append(stripped_line)
        logger.info(f"Lidos {len(ids)} IDs do arquivo '{filename}'.")
    except Exception as e:
        logger.error(f"Erro ao ler IDs do arquivo '{filename}': {e}")
    return ids

async def main():
    """Função principal para parsear argumentos, coletar dados e salvar."""
    parser = argparse.ArgumentParser(description="Coleta dados do PNCP para IDs específicos.")
    parser.add_argument(
        "--api",
        type=str,
        required=True,
        choices=API_ENDPOINTS.keys(),
        help=f"Tipo de API a ser consultada. Opções: {', '.join(API_ENDPOINTS.keys())}"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Nome do arquivo de saída (ex: dados.csv)"
    )
    args = parser.parse_args()

    # Inicializa o semáforo dentro da função main, associando-o ao loop de eventos correto
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    ids_to_process = read_ids_from_file()
    if not ids_to_process:
        logger.error("Nenhum ID para processar. Encerrando.")
        return

    logger.info(f"Iniciando coleta de dados para a API: {args.api}")
    data = await collect_data(args.api, ids_to_process, semaphore)

    if isinstance(data, pd.DataFrame) and not data.empty:
        # Salva DataFrame como CSV, usando ponto e vírgula como separador e UTF-8 BOM
        data.to_csv(args.output, index=False, sep=';', encoding='utf-8-sig')
        logger.info(f"Dados salvos com sucesso em '{args.output}' (CSV JSON).")
    elif isinstance(data, str) and data: # Caso seja um conteúdo CSV diretamente da API
        with open(args.output, 'w', encoding='utf-8-sig') as f:
            f.write(data)
        logger.info(f"Dados salvos com sucesso em '{args.output}' (CSV API).")
    else:
        logger.warning(f"Nenhum dado foi coletado ou gerado para salvar no arquivo '{args.output}'.")

if __name__ == "__main__":
    asyncio.run(main())
