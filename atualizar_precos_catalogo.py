import os
import asyncio
import aiohttp
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
import json
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

# --- Configuração Inicial ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
API_BASE_URL = "https://dadosabertos.compras.gov.br/precos/v1"
MAX_CONCURRENT_REQUESTS = 5
TIMEOUT = 30

# --- Funções de Banco de Dados ---

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    try:
        conn = psycopg2.connect(CONN_STRING)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None


def get_itens_catalogo_from_db(conn) -> List[Tuple[str, str]]:
    """
    Busca todos os itens de catálogo distintos do banco.
    Retorna lista de tuplas (codigo, tipo).
    """
    with conn.cursor() as cur:
        # Query simplificada - ajuste conforme sua estrutura real
        cur.execute("""
            SELECT DISTINCT
                codigo,
                tipo
            FROM itens_catalogo
            WHERE codigo IS NOT NULL
            AND tipo IS NOT NULL
            ORDER BY codigo;
        """)
        return cur.fetchall()


def upsert_precos_catalogo(
    conn,
    codigo_item: str,
    tipo_item: str,
    precos_data: List[Dict[str, Any]]
) -> int:
    """
    Faz upsert dos preços na tabela precos_catalogo.
    Retorna o número de registros inseridos/atualizados.
    """
    if not precos_data:
        return 0

    inserted_count = 0

    with conn.cursor() as cur:
        for preco in precos_data:
            try:
                # Extrai os dados do preço (ajuste conforme estrutura da API)
                descricao = preco.get('descricaoItem', '')
                unidade = preco.get('unidadeMedida', '')
                quantidade = preco.get('quantidade')
                valor_unitario = preco.get('valorUnitario')
                valor_total = preco.get('valorTotal')
                ni_fornecedor = preco.get('niFornecedor', '')
                nome_fornecedor = preco.get('nomeFornecedor', '')
                id_compra = preco.get('idCompra', '')
                data_compra = preco.get('dataCompra')

                # Converte strings para tipos apropriados
                try:
                    quantidade = float(quantidade) if quantidade else None
                    valor_unitario = float(valor_unitario) if valor_unitario else None
                    valor_total = float(valor_total) if valor_total else None
                except (ValueError, TypeError):
                    quantidade = None
                    valor_unitario = None
                    valor_total = None

                # SQL com ON CONFLICT para upsert
                cur.execute("""
                    INSERT INTO precos_catalogo (
                        codigo_item_catalogo,
                        tipo_item,
                        descricao_item,
                        unidade_medida,
                        quantidade_total,
                        valor_unitario,
                        valor_total,
                        ni_fornecedor,
                        nome_fornecedor,
                        id_compra,
                        data_compra,
                        data_atualizacao
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (codigo_item_catalogo, ni_fornecedor, id_compra, valor_unitario)
                    DO UPDATE SET
                        descricao_item = EXCLUDED.descricao_item,
                        unidade_medida = EXCLUDED.unidade_medida,
                        quantidade_total = EXCLUDED.quantidade_total,
                        valor_total = EXCLUDED.valor_total,
                        nome_fornecedor = EXCLUDED.nome_fornecedor,
                        data_compra = EXCLUDED.data_compra,
                        data_atualizacao = CURRENT_TIMESTAMP
                    WHERE precos_catalogo.data_atualizacao < CURRENT_TIMESTAMP - INTERVAL '1 hour';
                """, (
                    codigo_item,
                    tipo_item,
                    descricao,
                    unidade,
                    quantidade,
                    valor_unitario,
                    valor_total,
                    ni_fornecedor,
                    nome_fornecedor,
                    id_compra,
                    data_compra
                ))

                inserted_count += 1

            except psycopg2.Error as e:
                logging.error(
                    f"Erro ao fazer upsert do preço para item {codigo_item}: {e}"
                )
                conn.rollback()
                continue

    conn.commit()
    return inserted_count


# --- Funções de API Assíncrona ---

async def fetch_precos_item(
    session: aiohttp.ClientSession,
    codigo_item: str,
    tipo_item: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Busca os preços de um item específico da API.
    Retorna lista de preços ou None em caso de erro.
    """
    # Define o endpoint baseado no tipo
    if 'material' in tipo_item.lower():
        params = {'codigo_item_material': codigo_item}
    elif 'serviço' in tipo_item.lower():
        params = {'codigo_item_servico': codigo_item}
    else:
        logging.warning(f"Tipo '{tipo_item}' não reconhecido para item {codigo_item}")
        return None

    url = f"{API_BASE_URL}/precos"

    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT)
        ) as response:
            if response.status == 200:
                data = await response.json()
                # A API pode retornar um objeto com uma chave 'precos' ou diretamente uma lista
                precos = data.get('precos', data) if isinstance(data, dict) else data
                logging.info(
                    f"Encontrados {len(precos) if isinstance(precos, list) else 1} "
                    f"preços para item {codigo_item}"
                )
                return precos if isinstance(precos, list) else [precos]
            elif response.status == 404:
                logging.info(f"Nenhum preço encontrado para item {codigo_item}")
                return []
            else:
                logging.warning(
                    f"Status {response.status} ao buscar preços para item {codigo_item}"
                )
                return None

    except asyncio.TimeoutError:
        logging.error(f"Timeout ao buscar preços para item {codigo_item}")
        return None
    except aiohttp.ClientError as e:
        logging.error(f"Erro de conexão ao buscar preços para item {codigo_item}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON para item {codigo_item}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao buscar preços para item {codigo_item}: {e}")
        return None


async def process_itens_concorrently(
    itens: List[Tuple[str, str]],
    conn: psycopg2.extensions.connection
) -> Dict[str, int]:
    """
    Processa múltiplos itens de forma concorrente.
    Retorna estatísticas de processamento.
    """
    stats = {
        'total_itens': len(itens),
        'itens_processados': 0,
        'precos_inseridos': 0,
        'erros': 0
    }

    # Cria um semáforo para limitar requisições concorrentes
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_and_upsert(codigo: str, tipo: str) -> None:
        """Busca e faz upsert de um item."""
        async with semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    precos = await fetch_precos_item(session, codigo, tipo)

                    if precos is not None:
                        count = upsert_precos_catalogo(conn, codigo, tipo, precos)
                        stats['precos_inseridos'] += count
                        stats['itens_processados'] += 1
                        logging.info(
                            f"Item {codigo}: {count} preços inseridos/atualizados"
                        )
                    else:
                        stats['erros'] += 1

            except Exception as e:
                logging.error(f"Erro ao processar item {codigo}: {e}")
                stats['erros'] += 1

    # Cria tasks para todos os itens
    tasks = [fetch_and_upsert(codigo, tipo) for codigo, tipo in itens]

    # Executa todas as tasks concorrentemente
    await asyncio.gather(*tasks)

    return stats


# --- Função Principal ---

async def main():
    """Função principal que orquestra o processo."""
    conn = get_db_connection()
    if not conn:
        logging.error("Falha ao conectar ao banco de dados. Abortando.")
        return

    try:
        # Busca itens do catálogo
        itens_para_buscar = get_itens_catalogo_from_db(conn)
        logging.info(f"Encontrados {len(itens_para_buscar)} itens de catálogo para processar.")

        if not itens_para_buscar:
            logging.warning("Nenhum item encontrado para processar.")
            return

        # Processa itens de forma assíncrona
        stats = await process_itens_concorrently(itens_para_buscar, conn)

        # Log das estatísticas finais
        logging.info("=" * 60)
        logging.info("RESUMO DO PROCESSAMENTO")
        logging.info("=" * 60)
        logging.info(f"Total de itens: {stats['total_itens']}")
        logging.info(f"Itens processados com sucesso: {stats['itens_processados']}")
        logging.info(f"Preços inseridos/atualizados: {stats['precos_inseridos']}")
        logging.info(f"Erros encontrados: {stats['erros']}")
        logging.info("=" * 60)

    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

    finally:
        conn.close()
        logging.info("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    asyncio.run(main())