import os
import asyncio
import aiohttp
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
from decimal import Decimal, InvalidOperation
import pandas as pd
import io
import re
import random
from charset_normalizer import from_bytes
from ftfy import fix_text

# --- Configuração ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
LOTE_SIZE = 5
TIMEOUT_LOTE = 300.0  # 5 minutos
TIMEOUT_TOTAL = 120  # Timeout total da requisição
TIMEOUT_CONNECT = 30  # Timeout para estabelecer conexão
TIMEOUT_READ = 90  # Timeout para ler dados
MAX_RETRIES = 5

# User-Agents para rotação
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
]

# --- Funções Auxiliares ---

def normalizar_nome_coluna(nome: str) -> str:
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

def gerar_id_item_compra(codigo_uasg: str, modalidade: str, id_contratacao: str, numero_item: str) -> Optional[str]:
    """
    Gera o ID do item de compra no formato:
    unidade_uasg_codigo (6 dígitos) + modalidade (2 dígitos) + id_contratacao (9 dígitos) + numero_item (5 dígitos)
    
    Exemplo: 986001 + 03 + 900012024 + 00001 = 9860010390001202400001
    """
    try:
        codigo_uasg = str(codigo_uasg).strip()
        modalidade = str(modalidade).strip()
        id_contratacao = str(id_contratacao).strip()
        numero_item = str(numero_item).strip()
        
        if not all([codigo_uasg, modalidade, id_contratacao, numero_item]):
            return None
        
        codigo_uasg_fmt = codigo_uasg.zfill(6)
        modalidade_fmt = modalidade.zfill(2)
        id_contratacao_fmt = id_contratacao.zfill(9)
        numero_item_fmt = numero_item.zfill(5)
        
        id_item_compra = f"{codigo_uasg_fmt}{modalidade_fmt}{id_contratacao_fmt}{numero_item_fmt}"
        
        return id_item_compra
    except (ValueError, AttributeError):
        return None

def converter_valor_brasileiro(valor_str: Any) -> Optional[Decimal]:
    """
    Converte valores no formato brasileiro (162.800,5477) para Decimal.
    Preserva precisão exata de casas decimais.
    """
    if valor_str is None:
        return None
    
    if isinstance(valor_str, Decimal):
        return valor_str
    
    if isinstance(valor_str, (int, float)):
        try:
            return Decimal(str(valor_str))
        except InvalidOperation:
            return None
    
    if not isinstance(valor_str, str):
        valor_str = str(valor_str)
    
    try:
        valor_str = valor_str.strip()
        
        if not valor_str:
            return None
        
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        return Decimal(valor_str)
    except (ValueError, InvalidOperation, AttributeError):
        return None

def _decode_and_clean_csv(raw_bytes: bytes) -> str:
    try:
        probe = from_bytes(raw_bytes).best()
        decoded_text = str(probe) if probe else raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        decoded_text = raw_bytes.decode("latin-1", errors="replace")
    
    fixed_text_content = fix_text(decoded_text)
    lines = fixed_text_content.strip().splitlines()
    if lines and "totalRegistros" in lines[-1]:
        lines.pop()
    return "\n".join(lines)

# --- Funções de Banco de Dados ---

def get_db_connection():
    try:
        conn = psycopg2.connect(CONN_STRING)
        logger.info("Conexão com banco de dados estabelecida com sucesso")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Falha ao criar conexão inicial: {e}")
        return None

def get_itens_para_processar(conn) -> Dict[str, Dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT DISTINCT codigo, TRIM(LOWER(tipo)) as tipo FROM itens_catalogo WHERE codigo IS NOT NULL AND tipo IS NOT NULL;")
        itens_base = cur.fetchall()
        
        if not itens_base:
            return {}
        
        cur.execute("SELECT codigo_item_catalogo, MAX(data_compra) as ultima_data FROM precos_catalogo GROUP BY codigo_item_catalogo;")
        ultimas_datas = {row['codigo_item_catalogo']: row['ultima_data'] for row in cur}
        
        itens_para_processar = {
            str(item['codigo']): {
                'tipo': item['tipo'],
                'ultima_data': ultimas_datas.get(str(item['codigo']))
            } for item in itens_base
        }
        
        logger.info(f"Encontrados {len(itens_para_processar)} itens únicos para verificar.")
        return itens_para_processar

def sync_lote_precos_catalogo(conn_string: str, resultados_lote: List[Dict]) -> Tuple[int, int, int]:
    total_novos, total_atualizados, total_erros_db = 0, 0, 0
    
    logger.info(f"Iniciando salvamento de {len(resultados_lote)} itens no banco de dados...")
    
    for resultado in resultados_lote:
        conn = None
        try:
            codigo_item = resultado['codigo']
            tipo_item = resultado['tipo']
            precos_api = resultado['precos']

            if not precos_api:
                logger.info(f"Item {codigo_item}: Nenhum preço para processar")
                continue

            logger.info(f"Processando item {codigo_item} com {len(precos_api)} preços da API...")

            conn = psycopg2.connect(conn_string)
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                precos_validos = []
                precos_ignorados = 0
                erros_conversao = 0
                erros_id_item = 0
                
                for p in precos_api:
                    codigo_uasg = p.get('codigo_uasg') or ''
                    modalidade = p.get('modalidade') or ''
                    id_compra_api = p.get('id_compra') or ''
                    numero_item_compra = p.get('numero_item_compra') or ''
                    
                    id_item_compra = gerar_id_item_compra(
                        codigo_uasg,
                        modalidade,
                        id_compra_api,
                        numero_item_compra
                    )
                    
                    if not id_item_compra:
                        erros_id_item += 1
                        if erros_id_item <= 3:
                            logger.warning(f"Item {codigo_item}: Não foi possível gerar id_item_compra. UASG={codigo_uasg}, Mod={modalidade}, ID={id_compra_api}, NumItem={numero_item_compra}")
                        continue
                    
                    ni_fornecedor_api = (
                        p.get('ni_fornecedor') or
                        p.get('cnpj_vencedor') or 
                        p.get('cnpj_fornecedor') or 
                        p.get('cnpj') or
                        ''
                    )
                    
                    if not ni_fornecedor_api:
                        precos_ignorados += 1
                        continue
                    
                    valor_unitario_str = (
                        p.get('preco_unitario') or
                        p.get('valor_unitario_homologado') or 
                        p.get('valor_unitario') or 
                        p.get('valor') or
                        None
                    )
                    
                    valor_unitario = converter_valor_brasileiro(valor_unitario_str)
                    
                    if valor_unitario is None:
                        erros_conversao += 1
                        continue
                    
                    descricao = p.get('descricao_item') or p.get('descricao_item_catalogo') or p.get('descricao') or None
                    
                    unidade = (
                        p.get('sigla_unidade_medida') or
                        p.get('nome_unidade_medida') or
                        p.get('unidade_fornecimento') or 
                        p.get('unidade_medida') or 
                        p.get('unidade') or
                        None
                    )
                    
                    quantidade_str = p.get('quantidade') or p.get('quantidade_item') or None
                    quantidade = converter_valor_brasileiro(quantidade_str)
                    
                    valor_total_str = p.get('valor_total_homologado') or p.get('valor_total') or p.get('preco_total') or None
                    valor_total = converter_valor_brasileiro(valor_total_str)
                    
                    if valor_total is None and valor_unitario is not None and quantidade is not None:
                        try:
                            valor_total = valor_unitario * quantidade
                        except (ValueError, TypeError, InvalidOperation):
                            valor_total = None
                    
                    nome_fornecedor = (
                        p.get('nome_fornecedor') or
                        p.get('nome_vencedor') or 
                        p.get('fornecedor') or
                        None
                    )
                    
                    data_resultado = (
                        p.get('data_resultado') or
                        p.get('data_compra') or 
                        p.get('data') or
                        None
                    )
                    
                    precos_validos.append((
                        codigo_item,
                        tipo_item,
                        descricao,
                        unidade,
                        quantidade,
                        valor_unitario,
                        valor_total,
                        ni_fornecedor_api,
                        nome_fornecedor,
                        id_item_compra,
                        data_resultado,
                        datetime.now()
                    ))
                
                if precos_ignorados > 0:
                    logger.warning(f"Item {codigo_item}: {precos_ignorados} preços ignorados (NI fornecedor ausente)")
                
                if erros_conversao > 0:
                    logger.warning(f"Item {codigo_item}: {erros_conversao} preços com erro de conversão de valor")
                
                if erros_id_item > 0:
                    logger.warning(f"Item {codigo_item}: {erros_id_item} preços sem id_item_compra válido")
                
                logger.info(f"Item {codigo_item}: {len(precos_validos)} preços válidos para processar")
                
                if precos_validos:
                    logger.info(f"Executando UPSERT para {len(precos_validos)} preços do item {codigo_item}...")
                    
                    template = """
                        INSERT INTO precos_catalogo 
                        (codigo_item_catalogo, tipo_item, descricao_item, unidade_medida, 
                         quantidade_total, valor_unitario, valor_total, ni_fornecedor, 
                         nome_fornecedor, id_item_compra, data_compra, data_atualizacao)
                        VALUES %s
                        ON CONFLICT (codigo_item_catalogo, id_item_compra, ni_fornecedor)
                        DO UPDATE SET
                            tipo_item = EXCLUDED.tipo_item,
                            descricao_item = EXCLUDED.descricao_item,
                            unidade_medida = EXCLUDED.unidade_medida,
                            quantidade_total = EXCLUDED.quantidade_total,
                            valor_unitario = EXCLUDED.valor_unitario,
                            valor_total = EXCLUDED.valor_total,
                            nome_fornecedor = EXCLUDED.nome_fornecedor,
                            data_compra = EXCLUDED.data_compra,
                            data_atualizacao = EXCLUDED.data_atualizacao
                    """
                    
                    psycopg2.extras.execute_values(
                        cur,
                        template,
                        precos_validos,
                        page_size=1000
                    )
                    
                    total_novos += len(precos_validos)
                    logger.info(f"Item {codigo_item}: {len(precos_validos)} preços processados com sucesso!")
            
            conn.commit()
            logger.info(f"Item {codigo_item}: Transação commitada com sucesso")
            
        except Exception as e:
            logger.error(f"Erro de DB para item {resultado.get('codigo', 'N/A')}: {e}", exc_info=True)
            total_erros_db += 1
            if conn:
                conn.rollback()
                logger.warning(f"Rollback executado para item {resultado.get('codigo', 'N/A')}")
        finally:
            if conn:
                conn.close()
    
    if total_novos > 0:
        logger.info(f"LOTE COMPLETO: {total_novos} preços processados")
    else:
        logger.info(f"LOTE COMPLETO: Nenhuma alteração necessária no banco")
            
    return total_novos, total_atualizados, total_erros_db

# --- Funções de Coleta ---

async def fetch_precos_item(session: aiohttp.ClientSession, codigo_item: str, tipo_item: str, ultima_data: Optional[datetime]) -> Dict:
    tipo_normalizado = tipo_item.strip().lower()
    
    if 'material' in tipo_normalizado:
        base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"
    else:
        base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV"
    
    params = {
        'codigoItemCatalogo': codigo_item,
        'pagina': 1,
        'tamanhoPagina': 500
    }
    
    if ultima_data:
        data_inicio_busca = ultima_data + timedelta(days=1)
        params['dataInicio'] = data_inicio_busca.strftime('%d/%m/%Y')
        logger.info(f"Buscando item {codigo_item} (a partir de {params['dataInicio']})...")
    else:
        logger.info(f"Buscando item {codigo_item} (histórico completo)...")

    all_dfs, current_page, retries = [], 1, 0
    sucesso_coleta = True
    connection_errors = 0
    
    while True:
        params['pagina'] = current_page
        
        # MELHORIA 1: Rotação de User-Agent
        headers = {
            'accept': 'text/csv',
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        }
        
        try:
            # MELHORIA 2: Timeout de conexão separado
            timeout = aiohttp.ClientTimeout(
                total=TIMEOUT_TOTAL,
                connect=TIMEOUT_CONNECT,
                sock_read=TIMEOUT_READ
            )
            
            async with session.get(
                base_url,
                params=params,
                headers=headers,
                timeout=timeout,
                ssl=True
            ) as response:
                
                # Reset contador de erros de conexão em caso de sucesso
                connection_errors = 0
                
                if response.status == 429:
                    if retries < MAX_RETRIES:
                        wait_time = 5 * (2 ** retries)
                        logger.warning(f"Item {codigo_item}: Rate limit (429) na página {current_page}. Tentativa {retries+1}/{MAX_RETRIES}. Aguardando {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        logger.error(f"Item {codigo_item}: Rate limit excedido após {MAX_RETRIES} tentativas.")
                        sucesso_coleta = False
                        break
                
                if response.status == 503:
                    if retries < MAX_RETRIES:
                        wait_time = 10 * (2 ** retries)
                        logger.warning(f"Item {codigo_item}: Serviço indisponível (503) na página {current_page}. Aguardando {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        logger.error(f"Item {codigo_item}: Serviço indisponível após {MAX_RETRIES} tentativas.")
                        sucesso_coleta = False
                        break
                
                if response.status != 200:
                    logger.warning(f"Item {codigo_item}: Status {response.status} na página {current_page}")
                    break
                
                retries = 0
                content_bytes = await response.read()
                
                if not content_bytes:
                    break
                
                cleaned_csv_text = _decode_and_clean_csv(content_bytes)
                
                if len(cleaned_csv_text.strip().splitlines()) < 2:
                    break
                
                df_page = pd.read_csv(
                    io.StringIO(cleaned_csv_text),
                    sep=';',
                    on_bad_lines='warn',
                    engine='python',
                    dtype=str
                )
                
                if df_page.empty:
                    break
                
                all_dfs.append(df_page)
                current_page += 1
                
                # MELHORIA 4: Delay aleatório entre requisições
                delay = random.uniform(0.5, 1.5)
                await asyncio.sleep(delay)
        
        # MELHORIA 3: Retry específico para erros de conexão
        except (aiohttp.ClientError, aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
            connection_errors += 1
            
            if connection_errors <= MAX_RETRIES:
                wait_time = 5 * (2 ** connection_errors)
                logger.warning(f"Item {codigo_item}: Erro de conexão ({type(e).__name__}) na página {current_page}. Tentativa {connection_errors}/{MAX_RETRIES}. Aguardando {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Item {codigo_item}: Falha de conexão após {MAX_RETRIES} tentativas: {e}")
                sucesso_coleta = False
                break
        
        except Exception as e:
            logger.error(f"Erro inesperado para item {codigo_item}: {type(e).__name__} - {e}", exc_info=False)
            sucesso_coleta = False
            break
    
    precos_coletados = []
    if sucesso_coleta and all_dfs:
        try:
            full_df = pd.concat(all_dfs, ignore_index=True)
            full_df.columns = [normalizar_nome_coluna(col) for col in full_df.columns]
            full_df = full_df.where(pd.notna(full_df), None)
            precos_coletados = full_df.to_dict('records')
            logger.info(f"SUCESSO: {len(precos_coletados)} preços coletados para o item {codigo_item}")
        except Exception as e:
            logger.error(f"Erro ao concatenar/processar DataFrame para o item {codigo_item}: {e}")
            sucesso_coleta = False

    return {
        'codigo': codigo_item,
        'tipo': tipo_item,
        'precos': precos_coletados,
        'sucesso': sucesso_coleta
    }

# --- Orquestração Principal ---

async def processar_e_salvar_lotes(session, itens_para_processar_lista, stats):
    itens_falhados = []
    total_lotes = (len(itens_para_processar_lista) + LOTE_SIZE - 1) // LOTE_SIZE
    
    for i in range(0, len(itens_para_processar_lista), LOTE_SIZE):
        lote_atual = itens_para_processar_lista[i:i + LOTE_SIZE]
        lote_num = i//LOTE_SIZE + 1
        logger.info(f"\n{'='*60}")
        logger.info(f"PROCESSANDO LOTE {lote_num}/{total_lotes}")
        logger.info(f"{'='*60}\n")

        tasks = {
            asyncio.create_task(
                fetch_precos_item(session, codigo, detalhes['tipo'], detalhes['ultima_data']),
                name=f"Item-{codigo}"
            ) for codigo, detalhes in lote_atual
        }
        
        done, pending = await asyncio.wait(tasks, timeout=TIMEOUT_LOTE)
        
        resultados_coleta = []
        if done:
            for task in done:
                try:
                    resultados_coleta.append(task.result())
                except Exception as e:
                    logger.error(f"Erro ao obter resultado da tarefa {task.get_name()}: {e}")

        if pending:
            logger.warning(f"{len(pending)} tarefa(s) não concluída(s) dentro do timeout de {TIMEOUT_LOTE}s")
            for task in pending:
                logger.warning(f"  Tarefa pendente: {task.get_name()}")
                task.cancel()

        codigos_sucesso_coleta = {res['codigo'] for res in resultados_coleta if res['sucesso']}
        for codigo, detalhes in lote_atual:
            if codigo not in codigos_sucesso_coleta:
                itens_falhados.append((codigo, detalhes))

        resultados_validos = [res for res in resultados_coleta if res and res['sucesso']]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"SALVANDO LOTE {lote_num}/{total_lotes} NO BANCO DE DADOS")
        logger.info(f"{'='*60}\n")
        
        if resultados_validos:
            novos, atualizados, erros_db = sync_lote_precos_catalogo(CONN_STRING, resultados_validos)
            stats['precos_novos'] += novos
            stats['precos_atualizados'] += atualizados
            stats['erros'] += erros_db
            
            logger.info(f"\n{'='*60}")
            logger.info(f"LOTE {lote_num}/{total_lotes} CONCLUIDO")
            logger.info(f"   Processados: {novos} | Erros: {erros_db}")
            logger.info(f"{'='*60}\n")
        else:
            logger.warning(f"Nenhum resultado válido para salvar no lote {lote_num}/{total_lotes}")
        
        stats['itens_processados'] += len(lote_atual)
    
    return itens_falhados

async def main():
    logger.info("Iniciando sincronização de preços do catálogo...")
    
    conn = get_db_connection()
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados. Encerrando.")
        return
    
    try:
        itens_para_processar = get_itens_para_processar(conn)
    finally:
        conn.close()

    if not itens_para_processar:
        logger.info("Nenhum item para processar.")
        return

    stats = {
        'total_itens': len(itens_para_processar),
        'itens_processados': 0,
        'precos_novos': 0,
        'precos_atualizados': 0,
        'erros': 0
    }
    
    itens_lista = list(itens_para_processar.items())

    # Configuração otimizada do connector
    connector = aiohttp.TCPConnector(
        limit=10,  # Máximo de conexões simultâneas
        limit_per_host=5,  # Máximo por host
        ttl_dns_cache=300,  # Cache DNS por 5 minutos
        enable_cleanup_closed=True,
        force_close=False,  # Reutiliza conexões
        ssl=True
    )
    
    timeout = aiohttp.ClientTimeout(
        total=TIMEOUT_TOTAL,
        connect=TIMEOUT_CONNECT,
        sock_read=TIMEOUT_READ
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        trust_env=True
    ) as session:
        logger.info(f"\n{'='*60}")
        logger.info("INICIANDO PRIMEIRA PASSAGEM")
        logger.info(f"{'='*60}\n")
        
        itens_falhados_passo1 = await processar_e_salvar_lotes(session, itens_lista, stats)

        if itens_falhados_passo1:
            logger.info(f"\n{'='*60}")
            logger.info(f"INICIANDO RETENTATIVA PARA {len(itens_falhados_passo1)} ITENS")
            logger.info(f"{'='*60}\n")
            
            stats['itens_processados'] = 0
            itens_falhados_passo2 = await processar_e_salvar_lotes(session, itens_falhados_passo1, stats)
            
            if itens_falhados_passo2:
                logger.error(f"\n{'='*60}")
                logger.error("ITENS COM FALHA PERSISTENTE")
                logger.error(f"{'='*60}")
                for codigo, _ in itens_falhados_passo2:
                    logger.error(f"  Item {codigo} falhou em todas as tentativas")
                stats['erros'] += len(itens_falhados_passo2)

    logger.info(f"\n{'='*80}")
    logger.info("RESUMO FINAL DA SINCRONIZACAO")
    logger.info(f"{'='*80}")
    logger.info(f"  Total de Itens: {stats['total_itens']}")
    logger.info(f"  Itens Processados: {stats['itens_processados']}")
    logger.info(f"  Preços Processados: {stats['precos_novos']}")
    logger.info(f"  Erros: {stats['erros']}")
    logger.info(f"{'='*80}\n")

if __name__ == "__main__":
    asyncio.run(main())