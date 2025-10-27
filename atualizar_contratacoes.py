# Arquivo: atualizar_contratacoes.py (Versão Simplificada e Final)

import os
import aiohttp
import asyncio
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime, timezone

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
API_BASE_URL = "https://dadosabertos.compras.gov.br/modulo-contratacoes"
CONCURRENT_REQUESTS_LIMIT = 10

# --- Funções de Banco de Dados ---
def get_db_connection():
    if not CONN_STRING:
        logging.error("String de conexão COCKROACHDB_CONN_STRING não encontrada.")
        return None
    try:
        conn = psycopg2.connect(CONN_STRING)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def upsert_data(conn, table, columns, data, conflict_target, update_columns):
    if not data: return 0
    cols_str = ", ".join(columns)
    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
    conflict_str = ", ".join(conflict_target) if isinstance(conflict_target, list) else conflict_target
    query = f"INSERT INTO {table} ({cols_str}) VALUES %s ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};"
    with conn.cursor() as cur:
        try:
            extras.execute_values(cur, query, data)
            conn.commit()
            # Log um pouco menos verboso para não poluir
            if cur.rowcount > 0:
                logging.info(f"{cur.rowcount} registros afetados na tabela {table}.")
            return cur.rowcount
        except Exception as e:
            logging.error(f"Erro ao fazer upsert na tabela {table}: {e}")
            conn.rollback()
            return 0

# --- Funções de API Assíncronas ---
async def fetch_api_data_async(session, endpoint, params):
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json(content_type=None)
    except Exception as e:
        logging.error(f"Erro em fetch_api_data_async para {url}: {e}")
        return None

# --- Funções de Processamento e Persistência ---
def persistir_dados(conn, compra_data, itens_data, resultados_data):
    api_update_date_obj = None
    api_update_date_str = compra_data.get('dataAtualizacaoPncp')
    if api_update_date_str:
        try:
            api_update_date_obj = datetime.fromisoformat(api_update_date_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            logging.warning(f"Não foi possível converter a data da API: {api_update_date_str}")

    orgao_para_db = [(compra_data.get('codigoOrgao'), compra_data.get('orgaoEntidadeRazaoSocial'), compra_data.get('orgaoEntidadeEsferaId'), compra_data.get('orgaoEntidadePoderId'), compra_data.get('orgaoEntidadeCnpj'), compra_data.get('orgaoEntidadeRazaoSocial'))]
    upsert_data(conn, 'orgaos', ['codigo', 'nome', 'esfera', 'poder', 'cnpj', 'razao_social'], orgao_para_db, ['codigo'], ['nome', 'esfera', 'poder', 'cnpj', 'razao_social'])
    unidade_para_db = [(compra_data.get('unidadeOrgaoCodigoUnidade'), compra_data.get('unidadeOrgaoNomeUnidade'), compra_data.get('unidadeOrgaoMunicipioNome'), compra_data.get('unidadeOrgaoCodigoIbge'), compra_data.get('unidadeOrgaoUfSigla'), compra_data.get('codigoOrgao'), api_update_date_obj)]
    upsert_data(conn, 'unidades_uasg', ['codigo', 'nome', 'municipio_nome', 'municipio_codigo_ibge', 'uf_sigla', 'codigo_orgao', 'data_atualizacao'], unidade_para_db, ['codigo'], ['nome', 'municipio_nome', 'municipio_codigo_ibge', 'uf_sigla', 'codigo_orgao', 'data_atualizacao'])
    id_compra = compra_data.get('idCompra')
    id_contratacao = id_compra[-9:] if id_compra else None
    compra_para_db = [(id_compra, id_contratacao, compra_data.get('unidadeOrgaoCodigoUnidade'), compra_data.get('codigoModalidade'), compra_data.get('numeroControlePNCP'), compra_data.get('processo'), compra_data.get('objetoCompra'), compra_data.get('srp'), compra_data.get('situacaoCompraNomePncp'), compra_data.get('valorTotalEstimado'), compra_data.get('valorTotalHomologado'), compra_data.get('dataInclusaoPncp'), api_update_date_obj, compra_data.get('dataPublicacaoPncp'), compra_data.get('dataAberturaPropostaPncp'), compra_data.get('dataEncerramentoPropostaPncp'), compra_data.get('contratacaoExcluida', False))]
    compra_cols = ['id', 'id_contratacao', 'unidade_uasg_codigo', 'modalidade_codigo', 'numero_controle_pncp', 'processo', 'objeto_compra', 'srp', 'situacao_compra_pncp', 'valor_total_estimado', 'valor_total_homologado', 'data_inclusao_pncp', 'data_atualizacao_pncp', 'data_publicacao_pncp', 'data_abertura_proposta', 'data_encerramento_proposta', 'contratacao_excluida']
    upsert_data(conn, 'compras', compra_cols, compra_para_db, ['id'], [col for col in compra_cols if col != 'id'])
    if itens_data:
        catalogo_para_db = list(set([(item.get('codItemCatalogo'), item.get('descricaoResumida'), item.get('materialOuServicoNome')) for item in itens_data if item.get('codItemCatalogo')]))
        upsert_data(conn, 'itens_catalogo', ['codigo', 'descricao', 'tipo'], catalogo_para_db, ['codigo'], ['descricao', 'tipo'])
        itens_para_db = [(item.get('idCompraItem'), item.get('idCompra'), item.get('codItemCatalogo'), item.get('numeroItemCompra'), item.get('numeroItemPncp'), item.get('descricaodetalhada'), item.get('unidadeMedida'), item.get('quantidade'), item.get('valorUnitarioEstimado'), item.get('valorTotal'), item.get('criterioJulgamentoNome'), item.get('situacaoCompraItemNome'), item.get('temResultado'), item.get('dataAtualizacaoPncp')) for item in itens_data]
        item_cols = ['id', 'id_compra', 'item_catalogo_codigo', 'numero_item_compra', 'numero_item_pncp', 'descricao_item', 'unidade_medida', 'quantidade', 'valor_unitario_estimado', 'valor_total_estimado', 'criterio_julgamento', 'situacao_item', 'tem_resultado', 'data_atualizacao_item']
        upsert_data(conn, 'itens_compra', item_cols, itens_para_db, ['id'], [col for col in item_cols if col != 'id'])
    if resultados_data:
        fornecedores_para_db = list(set([(res.get('niFornecedor'), res.get('nomeRazaoSocialFornecedor'), res.get('tipoPessoa', ''), res.get('porteFornecedorId'), res.get('porteFornecedorNome')) for res in resultados_data if res.get('niFornecedor')]))
        upsert_data(conn, 'fornecedores', ['ni', 'nome_razao_social', 'tipo_pessoa', 'porte_id', 'porte_nome'], fornecedores_para_db, ['ni'], ['nome_razao_social', 'tipo_pessoa', 'porte_id', 'porte_nome'])
        resultados_para_db = [(res.get('idCompraItem'), res.get('sequencialResultado'), res.get('niFornecedor'), res.get('ordemClassificacaoSrp'), res.get('quantidadeHomologada'), res.get('valorUnitarioHomologado'), res.get('valorTotalHomologado'), res.get('percentualDesconto'), res.get('situacaoCompraItemResultadoNome'), res.get('motivoCancelamento'), res.get('dataResultadoPncp')) for res in resultados_data]
        res_cols = ['id_item_compra', 'sequencial_resultado', 'ni_fornecedor', 'ordem_classificacao_srp', 'quantidade_homologada', 'valor_unitario_homologado', 'valor_total_homologado', 'percentual_desconto', 'situacao_resultado_nome', 'motivo_cancelamento', 'data_resultado_pncp']
        upsert_data(conn, 'resultados_itens', res_cols, resultados_para_db, ['id_item_compra', 'sequencial_resultado'], [col for col in res_cols if col not in ['id_item_compra', 'sequencial_resultado']])

async def processar_contratacao_async(session, semaphore, conn, id_compra):
    async with semaphore:
        logging.info(f"Processando idCompra: {id_compra}")
        
        # <<< LÓGICA DE COMPARAÇÃO REMOVIDA >>>
        # Agora, sempre buscamos os dados e deixamos o banco decidir se atualiza.
        
        params = {'tipo': 'idCompra', 'codigo': id_compra}
        contratacao_json = await fetch_api_data_async(session, "1.1_consultarContratacoes_PNCP_14133_Id", params)
        
        if not contratacao_json or not contratacao_json.get('resultado'):
            logging.warning(f"Nenhuma contratação encontrada para idCompra: {id_compra}")
            return

        compra = contratacao_json['resultado'][0]
        
        logging.info(f"Buscando sub-dados para idCompra {id_compra}...")

        itens_task = fetch_api_data_async(session, "2.1_consultarItensContratacoes_PNCP_14133_Id", params)
        resultados_task = fetch_api_data_async(session, "3.1_consultarResultadoItensContratacoes_PNCP_14133_Id", params)
        
        itens_json, resultados_json = await asyncio.gather(itens_task, resultados_task)
        
        persistir_dados(conn, compra, itens_json.get('resultado', []), resultados_json.get('resultado', []))
        logging.info(f"Processo de persistência para idCompra {id_compra} concluído.")

# --- Função Principal Assíncrona ---
async def main_async():
    conn = get_db_connection()
    if not conn: return
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
    async with aiohttp.ClientSession() as session:
        try:
            with open('idCompra_lista.txt', 'r') as f:
                id_compras = [line.strip() for line in f if line.strip()]
            logging.info(f"Encontrados {len(id_compras)} IDs de compra para processar.")
            tasks = [processar_contratacao_async(session, semaphore, conn, id_compra) for id_compra in id_compras]
            await asyncio.gather(*tasks)
        except FileNotFoundError:
            logging.error("Arquivo 'idCompra_lista.txt' não encontrado.")
        finally:
            if conn:
                conn.close()
                logging.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    asyncio.run(main_async())