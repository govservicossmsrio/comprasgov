# Arquivo: atualizar_contratacoes.py (Versão com Correção de Duplicatas no Catálogo)

import os
import aiohttp
import asyncio
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
API_BASE_URL = "https://dadosabertos.compras.gov.br/modulo-contratacoes"

CONCURRENT_REQUESTS_LIMIT = 2
DELAY_BETWEEN_TASKS = 0.1
TIMEOUT_API = aiohttp.ClientTimeout(total=90)

ARQUIVO_ATIVAS = 'idCompra_ativas.txt'
ARQUIVO_ARQUIVADAS = 'idCompra_arquivadas.txt'

STATUS_ITEM_RESOLVIDO = ("homologado", "fracassado", "deserto", "anulado/revogado/cancelado")

# --- Funções ---

def get_db_connection():
    # (código sem alterações)
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

def verificar_se_compra_esta_concluida_no_db(conn, id_compra):
    # (código sem alterações)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM itens_compra WHERE id_compra = %s);", (id_compra,))
            tem_itens = cur.fetchone()[0]
            if not tem_itens:
                logging.info(f"Compra {id_compra} é nova ou não tem itens no DB. Não será arquivada por enquanto.")
                return False

            query = """
                SELECT EXISTS (
                    SELECT 1 
                    FROM itens_compra 
                    WHERE id_compra = %s 
                    AND lower(situacao_item) NOT IN %s
                );
            """
            cur.execute(query, (id_compra, STATUS_ITEM_RESOLVIDO))
            tem_item_ativo = cur.fetchone()[0]
            
            if tem_item_ativo:
                logging.info(f"Compra {id_compra} ainda tem itens ativos. Verificação de API necessária.")
                return False
            else:
                logging.info(f"Todos os itens da compra {id_compra} estão resolvidos. Marcada para arquivamento.")
                return True
                
    except psycopg2.Error as e:
        logging.error(f"Erro de DB ao verificar status da compra {id_compra}: {e}. A compra não será arquivada por segurança.")
        return False

def upsert_data(conn, table, columns, data, conflict_target, update_columns):
    # (código sem alterações)
    if not data: return 0
    cols_str = ", ".join(columns)
    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
    conflict_str = ", ".join(conflict_target) if isinstance(conflict_target, list) else conflict_target
    query = f"INSERT INTO {table} ({cols_str}) VALUES %s ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};"
    with conn.cursor() as cur:
        try:
            extras.execute_values(cur, query, data)
            conn.commit()
            if cur.rowcount > 0:
                logging.info(f"{cur.rowcount} registros afetados na tabela {table}.")
            return cur.rowcount
        except Exception as e:
            logging.error(f"Erro ao fazer upsert na tabela {table}: {e}")
            conn.rollback()
            return 0

async def fetch_api_data_async(session, endpoint, params):
    # (código sem alterações)
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        async with session.get(url, params=params, timeout=TIMEOUT_API) as response:
            if response.status == 429:
                logging.error(f"Erro 429 (Too Many Requests) para a URL: {response.url}. O servidor está sobrecarregado.")
                return None
            response.raise_for_status()
            return await response.json(content_type=None)
    except asyncio.TimeoutError:
        logging.error(f"Timeout ao chamar a API {url}")
        return None
    except aiohttp.ClientError as e:
        logging.error(f"Erro de cliente em fetch_api_data_async para {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado em fetch_api_data_async para {url}: {e}")
        return None

# <<< MUDANÇA APLICADA AQUI >>>
def persistir_dados(conn, compra_data, itens_data, resultados_data):
    # (código de data, orgao, unidade, compra sem alterações)
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
        # <<< CORREÇÃO APLICADA AQUI >>>
        # Garante que estamos lidando com uma lista única de códigos de catálogo
        catalogo_unico = {item['codItemCatalogo']: item for item in itens_data if item.get('codItemCatalogo')}.values()
        catalogo_para_db = [(item.get('codItemCatalogo'), item.get('descricaoResumida'), item.get('materialOuServicoNome')) for item in catalogo_unico]
        upsert_data(conn, 'itens_catalogo', ['codigo', 'descricao', 'tipo'], catalogo_para_db, ['codigo'], ['descricao', 'tipo'])
        
        # A lógica de limpeza para itens_compra já estava correta, mas a mantemos por segurança
        itens_unicos = {item['idCompraItem']: item for item in itens_data}.values()
        itens_para_db = [(item.get('idCompraItem'), item.get('idCompra'), item.get('codItemCatalogo'), item.get('numeroItemCompra'), item.get('numeroItemPncp'), item.get('descricaodetalhada'), item.get('unidadeMedida'), item.get('quantidade'), item.get('valorUnitarioEstimado'), item.get('valorTotal'), item.get('criterioJulgamentoNome'), item.get('situacaoCompraItemNome'), item.get('temResultado'), item.get('dataAtualizacaoPncp')) for item in itens_unicos]
        item_cols = ['id', 'id_compra', 'item_catalogo_codigo', 'numero_item_compra', 'numero_item_pncp', 'descricao_item', 'unidade_medida', 'quantidade', 'valor_unitario_estimado', 'valor_total_estimado', 'criterio_julgamento', 'situacao_item', 'tem_resultado', 'data_atualizacao_item']
        upsert_data(conn, 'itens_compra', item_cols, itens_para_db, ['id'], [col for col in item_cols if col != 'id'])
        
    if resultados_data:
        # (código sem alterações)
        resultados_unicos = {f"{res['idCompraItem']}-{res['sequencialResultado']}": res for res in resultados_data}.values()
        fornecedores_para_db = list(set([(res.get('niFornecedor'), res.get('nomeRazaoSocialFornecedor'), res.get('tipoPessoa', ''), res.get('porteFornecedorId'), res.get('porteFornecedorNome')) for res in resultados_unicos if res.get('niFornecedor')]))
        upsert_data(conn, 'fornecedores', ['ni', 'nome_razao_social', 'tipo_pessoa', 'porte_id', 'porte_nome'], fornecedores_para_db, ['ni'], ['nome_razao_social', 'tipo_pessoa', 'porte_id', 'porte_nome'])
        resultados_para_db = [(res.get('idCompraItem'), res.get('sequencialResultado'), res.get('niFornecedor'), res.get('ordemClassificacaoSrp'), res.get('quantidadeHomologada'), res.get('valorUnitarioHomologado'), res.get('valorTotalHomologado'), res.get('percentualDesconto'), res.get('situacaoCompraItemResultadoNome'), res.get('motivoCancelamento'), res.get('dataResultadoPncp')) for res in resultados_unicos]
        res_cols = ['id_item_compra', 'sequencial_resultado', 'ni_fornecedor', 'ordem_classificacao_srp', 'quantidade_homologada', 'valor_unitario_homologado', 'valor_total_homologado', 'percentual_desconto', 'situacao_resultado_nome', 'motivo_cancelamento', 'data_resultado_pncp']
        upsert_data(conn, 'resultados_itens', res_cols, resultados_para_db, ['id_item_compra', 'sequencial_resultado'], [col for col in res_cols if col not in ['id_item_compra', 'sequencial_resultado']])

# O resto do script (processar_contratacao_ativa_async, main_async) permanece o mesmo.
async def processar_contratacao_ativa_async(session, semaphore, conn, id_compra):
    async with semaphore:
        logging.info(f"Processando API para idCompra ativa: {id_compra}")
        params = {'tipo': 'idCompra', 'codigo': id_compra}
        
        contratacao_task = fetch_api_data_async(session, "1.1_consultarContratacoes_PNCP_14133_Id", params)
        itens_task = fetch_api_data_async(session, "2.1_consultarItensContratacoes_PNCP_14133_Id", params)
        resultados_task = fetch_api_data_async(session, "3.1_consultarResultadoItensContratacoes_PNCP_14133_Id", params)
        contratacao_json, itens_json, resultados_json = await asyncio.gather(contratacao_task, itens_task, resultados_task)
        
        if not contratacao_json or not contratacao_json.get('resultado'):
            logging.warning(f"Não foi possível obter dados da contratação principal para {id_compra}. A atualização falhou.")
            return

        compra = contratacao_json['resultado'][0]
        itens_data = itens_json.get('resultado', []) if itens_json else []
        resultados_data = resultados_json.get('resultado', []) if resultados_json else []
        
        persistir_dados(conn, compra, itens_data, resultados_data)
        logging.info(f"Processo de persistência para idCompra {id_compra} concluído.")

async def main_async():
    conn = None
    try:
        conn = get_db_connection()
        if not conn: return
        
        try:
            with open(ARQUIVO_ATIVAS, 'r') as f:
                id_compras_todas = {line.strip() for line in f if line.strip()}
        except FileNotFoundError:
            logging.error(f"Arquivo '{ARQUIVO_ATIVAS}' não encontrado. Criando um arquivo vazio.")
            id_compras_todas = set()
            open(ARQUIVO_ATIVAS, 'w').close()

        if not id_compras_todas:
            logging.info("Nenhuma compra ativa para processar.")
            return

        logging.info(f"Encontrados {len(id_compras_todas)} IDs de compra para verificação.")
        
        ids_para_arquivar = set()
        ids_para_processar_api = set()

        for id_compra in id_compras_todas:
            if verificar_se_compra_esta_concluida_no_db(conn, id_compra):
                ids_para_arquivar.add(id_compra)
            else:
                ids_para_processar_api.add(id_compra)
        
        if ids_para_processar_api:
            logging.info(f"Iniciando processamento de API para {len(ids_para_processar_api)} compras ativas.")
            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
            async with aiohttp.ClientSession() as session:
                tasks = []
                for id_compra in ids_para_processar_api:
                    task = asyncio.create_task(processar_contratacao_ativa_async(session, semaphore, conn, id_compra))
                    tasks.append(task)
                    await asyncio.sleep(DELAY_BETWEEN_TASKS)
                
                await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logging.info("Nenhuma compra ativa necessitando de atualização via API nesta execução.")

        if ids_para_arquivar:
            logging.info(f"Arquivando {len(ids_para_arquivar)} IDs de compra concluídos.")
            ids_ativos_final = id_compras_todas - ids_para_arquivar
            
            with open(ARQUIVO_ATIVAS, 'w') as f:
                for id_c in sorted(list(ids_ativos_final)):
                    f.write(id_c + '\n')
            with open(ARQUIVO_ARQUIVADAS, 'a') as f:
                for id_c in sorted(list(ids_para_arquivar)):
                    f.write(id_c + '\n')
            logging.info("Arquivos de listas de IDs atualizados.")
        else:
            logging.info("Nenhuma compra nova para arquivar nesta execução.")
            
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    asyncio.run(main_async())