import os
import asyncio
import aiohttp
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
import io
import re
import math
from charset_normalizer import from_bytes
from ftfy import fix_text

# --- Configuração ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('precos_sync.log'),
        logging.StreamHandler()
    ]
)
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
LOTE_SIZE = 5
TIMEOUT_LOTE = 180.0  # 3 minutos
TIMEOUT = 90
MAX_RETRIES = 5

# --- Mapeamento de Colunas ---
COLUMN_MAPPING = {
    'descricao_item_catalogo': [
        'descricao_item_catalogo', 'descricao_do_item', 'item_descricao', 
        'descricao_item', 'descricao'
    ],
    'unidade_fornecimento': [
        'unidade_fornecimento', 'unidade_medida', 'unidade', 'un_medida'
    ],
    'quantidade_item': [
        'quantidade_item', 'quantidade', 'qtd', 'qtd_item'
    ],
    'valor_unitario_homologado': [
        'valor_unitario_homologado', 'valor_unitario', 'preco_unitario', 
        'vlr_unitario', 'valor_unit'
    ],
    'valor_total_homologado': [
        'valor_total_homologado', 'valor_total', 'preco_total', 
        'vlr_total', 'valor_tot'
    ],
    'cnpj_vencedor': [
        'cnpj_vencedor', 'cnpj_fornecedor', 'documento_vencedor', 
        'ni_fornecedor', 'cnpj'
    ],
    'nome_vencedor': [
        'nome_vencedor', 'nome_fornecedor', 'razao_social_vencedor', 
        'fornecedor', 'nome_empresa'
    ],
    'numero_compra': [
        'numero_compra', 'numero_licitacao', 'id_compra', 
        'num_compra', 'compra_id'
    ],
    'data_resultado': [
        'data_resultado', 'data_homologacao', 'data_compra', 
        'dt_resultado', 'data_result'
    ]
}

# --- Funções Auxiliares ---

def normalizar_nome_coluna(nome: str) -> str:
    """Normaliza nomes de colunas para padrão snake_case"""
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

def get_mapped_value(record: Dict, field_name: str) -> Any:
    """Busca valor usando mapeamento de colunas"""
    possible_names = COLUMN_MAPPING.get(field_name, [field_name])
    
    for name in possible_names:
        # Tenta nome original
        if name in record and record[name] is not None:
            value = record[name]
            if isinstance(value, str) and value.strip():
                return value.strip()
            elif not isinstance(value, str):
                return value
        
        # Tenta nome normalizado
        normalized_name = normalizar_nome_coluna(name)
        if normalized_name in record and record[normalized_name] is not None:
            value = record[normalized_name]
            if isinstance(value, str) and value.strip():
                return value.strip()
            elif not isinstance(value, str):
                return value
    
    return None

def safe_get(data: Dict, key: str, default: Any = None) -> Any:
    """Função auxiliar para obter valores seguros"""
    value = get_mapped_value(data, key) if key in COLUMN_MAPPING else data.get(key, default)
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return default
    return value

def safe_float(value: Any, default: float = 0.0) -> float:
    """Conversão segura para float"""
    if value is None:
        return default
    
    try:
        # Remove caracteres não numéricos exceto ponto e vírgula
        if isinstance(value, str):
            # Remove espaços e caracteres especiais
            clean_value = re.sub(r'[^\d,.-]', '', value.strip())
            # Substitui vírgula por ponto (formato brasileiro)
            clean_value = clean_value.replace(',', '.')
            # Remove pontos extras (mantém apenas o último como decimal)
            parts = clean_value.split('.')
            if len(parts) > 2:
                clean_value = ''.join(parts[:-1]) + '.' + parts[-1]
            return float(clean_value) if clean_value else default
        else:
            return float(value)
    except (ValueError, TypeError, AttributeError):
        logging.warning(f"Erro ao converter valor para float: {value}")
        return default

def safe_int(value: Any, default: int = 0) -> int:
    """Conversão segura para int"""
    try:
        if isinstance(value, str):
            clean_value = re.sub(r'[^\d]', '', value.strip())
            return int(clean_value) if clean_value else default
        else:
            return int(float(value)) if value is not None else default
    except (ValueError, TypeError):
        return default

def safe_date(date_str: Any) -> Optional[datetime]:
    """Conversão segura para data"""
    if not date_str:
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # Formatos possíveis
        formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d']
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        logging.warning(f"Formato de data não reconhecido: {date_str}")
        return None
        
    except (ValueError, TypeError, AttributeError):
        logging.warning(f"Erro ao converter data: {date_str}")
        return None

def _decode_and_clean_csv(raw_bytes: bytes) -> str:
    """Decodifica e limpa conteúdo CSV"""
    try:
        probe = from_bytes(raw_bytes).best()
        decoded_text = str(probe) if probe else raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        decoded_text = raw_bytes.decode("latin-1", errors="replace")
    
    fixed_text_content = fix_text(decoded_text)
    lines = fixed_text_content.strip().splitlines()
    
    # Remove linha de total se existir
    if lines and "totalRegistros" in lines[-1]:
        lines.pop()
    
    return "\n".join(lines)

# --- Funções de Banco de Dados ---

def get_db_connection():
    """Cria conexão com o banco de dados"""
    try:
        conn = psycopg2.connect(CONN_STRING)
        logging.info("Conexão com banco estabelecida com sucesso")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Falha ao criar conexão inicial: {e}")
        return None

def verificar_schema_banco(conn) -> bool:
    """Verifica se o schema do banco está correto"""
    try:
        with conn.cursor() as cur:
            # Verificar se a tabela existe
            cur.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'precos_catalogo'
                ORDER BY ordinal_position
            """)
            
            colunas = cur.fetchall()
            if not colunas:
                logging.error("ERRO CRÍTICO: Tabela 'precos_catalogo' não encontrada!")
                return False
            
            logging.info("Schema da tabela precos_catalogo verificado:")
            for col in colunas:
                logging.info(f"  - {col[0]}: {col[1]} (nullable: {col[2]})")
            
            # Verificar alguns registros
            cur.execute("SELECT COUNT(*) FROM precos_catalogo")
            total = cur.fetchone()[0]
            logging.info(f"Total de registros na tabela: {total}")
            
            # Verificar tabela de itens
            cur.execute("SELECT COUNT(*) FROM itens_catalogo")
            total_itens = cur.fetchone()[0]
            logging.info(f"Total de itens no catálogo: {total_itens}")
            
            return True
            
    except Exception as e:
        logging.error(f"Erro ao verificar schema: {e}")
        return False

def get_itens_para_processar(conn) -> Dict[str, Dict]:
    """Obtém lista de itens para processar"""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT codigo, TRIM(LOWER(tipo)) as tipo 
                FROM itens_catalogo 
                WHERE codigo IS NOT NULL AND tipo IS NOT NULL
                ORDER BY codigo
            """)
            itens_base = cur.fetchall()
            
            if not itens_base:
                logging.warning("Nenhum item encontrado na tabela itens_catalogo")
                return {}
            
            logging.info(f"Encontrados {len(itens_base)} itens base no catálogo")
            
            # Buscar últimas datas de sincronização
            cur.execute("""
                SELECT codigo_item_catalogo, MAX(data_compra) as ultima_data 
                FROM precos_catalogo 
                GROUP BY codigo_item_catalogo
            """)
            ultimas_datas = {row['codigo_item_catalogo']: row['ultima_data'] for row in cur}
            
            logging.info(f"Encontradas datas de sincronização para {len(ultimas_datas)} itens")
            
            itens_para_processar = {}
            for item in itens_base:
                codigo_str = str(item['codigo'])
                itens_para_processar[codigo_str] = {
                    'tipo': item['tipo'],
                    'ultima_data': ultimas_datas.get(codigo_str)
                }
            
            logging.info(f"Preparados {len(itens_para_processar)} itens únicos para verificar")
            return itens_para_processar
            
    except Exception as e:
        logging.error(f"Erro ao obter itens para processar: {e}")
        return {}

def sync_lote_precos_catalogo(conn_string: str, resultados_lote: List[Dict]) -> Tuple[int, int, int]:
    """Sincroniza lote de preços com o banco de dados"""
    total_novos, total_atualizados, total_erros_db = 0, 0, 0
    
    logging.info(f"Iniciando sincronização de lote com {len(resultados_lote)} itens")
    
    for resultado in resultados_lote:
        conn = None
        try:
            codigo_item = str(resultado['codigo'])
            tipo_item = str(resultado['tipo'])
            precos_api = resultado['precos']

            logging.info(f"Processando item {codigo_item}: {len(precos_api)} preços da API")

            if not precos_api:
                logging.warning(f"Item {codigo_item}: Nenhum preço encontrado na API")
                continue

            conn = psycopg2.connect(conn_string)
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Verificar quantos registros existem no banco
                cur.execute("""
                    SELECT COUNT(*) as total 
                    FROM precos_catalogo 
                    WHERE codigo_item_catalogo = %s
                """, (codigo_item,))
                count_db = cur.fetchone()['total']
                logging.info(f"Item {codigo_item}: {count_db} registros existentes no banco")
                
                # Buscar registros existentes
                cur.execute("""
                    SELECT id_compra, ni_fornecedor, valor_unitario, data_compra
                    FROM precos_catalogo 
                    WHERE codigo_item_catalogo = %s
                """, (codigo_item,))
                precos_db_rows = cur.fetchall()
                
                # Criar mapa de registros existentes
                precos_db_map = {}
                for row in precos_db_rows:
                    chave = f"{row['id_compra']}-{row['ni_fornecedor']}"
                    precos_db_map[chave] = row
                
                precos_para_inserir, precos_para_atualizar = [], []
                registros_processados = 0
                registros_invalidos = 0
                
                for i, p in enumerate(precos_api):
                    registros_processados += 1
                    
                    # Extrair dados obrigatórios
                    id_compra_api = safe_get(p, 'numero_compra', '')
                    ni_fornecedor_api = safe_get(p, 'cnpj_vencedor', '')
                    valor_unitario_api = safe_float(safe_get(p, 'valor_unitario_homologado'))
                    
                    # Validar dados obrigatórios
                    if not id_compra_api or not ni_fornecedor_api:
                        registros_invalidos += 1
                        logging.debug(f"Item {codigo_item}, registro {i}: Dados obrigatórios faltando (id_compra: '{id_compra_api}', cnpj: '{ni_fornecedor_api}')")
                        continue
                    
                    if valor_unitario_api <= 0:
                        registros_invalidos += 1
                        logging.debug(f"Item {codigo_item}, registro {i}: Valor unitário inválido ({valor_unitario_api})")
                        continue
                    
                    # Verificar se registro já existe
                    chave_api = f"{id_compra_api}-{ni_fornecedor_api}"
                    preco_existente = precos_db_map.get(chave_api)
                    
                    if not preco_existente:
                        precos_para_inserir.append(p)
                        logging.debug(f"Item {codigo_item}: Novo registro para inserir (chave: {chave_api})")
                    else:
                        # Comparar valores para atualização
                        valor_db = float(preco_existente['valor_unitario']) if preco_existente['valor_unitario'] is not None else 0.0
                        
                        if not math.isclose(valor_unitario_api, valor_db, rel_tol=1e-6):
                            precos_para_atualizar.append(p)
                            logging.debug(f"Item {codigo_item}: Registro para atualizar (chave: {chave_api}, valor_api: {valor_unitario_api}, valor_db: {valor_db})")
                
                logging.info(f"Item {codigo_item}: Processados {registros_processados} registros, {registros_invalidos} inválidos, {len(precos_para_inserir)} para inserir, {len(precos_para_atualizar)} para atualizar")
                
                # INSERÇÕES
                if precos_para_inserir:
                    dados_insert = []
                    for p in precos_para_inserir:
                        dados_insert.append((
                            codigo_item,
                            tipo_item,
                            safe_get(p, 'descricao_item_catalogo', '')[:500],  # Limitar tamanho
                            safe_get(p, 'unidade_fornecimento', '')[:50],
                            safe_float(safe_get(p, 'quantidade_item')),
                            safe_float(safe_get(p, 'valor_unitario_homologado')),
                            safe_float(safe_get(p, 'valor_total_homologado')),
                            safe_get(p, 'cnpj_vencedor', '')[:20],
                            safe_get(p, 'nome_vencedor', '')[:200],
                            safe_get(p, 'numero_compra', '')[:50],
                            safe_date(safe_get(p, 'data_resultado')),
                            datetime.now()
                        ))
                    
                    try:
                        psycopg2.extras.execute_values(
                            cur, 
                            """INSERT INTO precos_catalogo 
                               (codigo_item_catalogo, tipo_item, descricao_item, unidade_medida, 
                                quantidade_total, valor_unitario, valor_total, ni_fornecedor, 
                                nome_fornecedor, id_compra, data_compra, data_atualizacao) 
                               VALUES %s
                               ON CONFLICT (codigo_item_catalogo, id_compra, ni_fornecedor) 
                               DO NOTHING""", 
                            dados_insert
                        )
                        
                        # Verificar quantos foram realmente inseridos
                        rows_affected = cur.rowcount
                        total_novos += rows_affected
                        logging.info(f"Item {codigo_item}: {rows_affected} registros inseridos com sucesso")
                        
                    except Exception as e:
                        logging.error(f"Item {codigo_item}: Erro na inserção: {e}")
                        raise

                # ATUALIZAÇÕES
                if precos_para_atualizar:
                    dados_update = []
                    for p in precos_para_atualizar:
                        dados_update.append((
                            safe_get(p, 'descricao_item_catalogo', '')[:500],
                            safe_get(p, 'unidade_fornecimento', '')[:50],
                            safe_float(safe_get(p, 'quantidade_item')),
                            safe_float(safe_get(p, 'valor_unitario_homologado')),
                            safe_float(safe_get(p, 'valor_total_homologado')),
                            safe_get(p, 'nome_vencedor', '')[:200],
                            safe_date(safe_get(p, 'data_resultado')),
                            codigo_item,
                            safe_get(p, 'numero_compra', '')[:50],
                            safe_get(p, 'cnpj_vencedor', '')[:20]
                        ))
                    
                    try:
                        psycopg2.extras.execute_batch(
                            cur, 
                            """UPDATE precos_catalogo SET 
                               descricao_item = %s, unidade_medida = %s, quantidade_total = %s, 
                               valor_unitario = %s, valor_total = %s, nome_fornecedor = %s, 
                               data_compra = %s, data_atualizacao = CURRENT_TIMESTAMP 
                               WHERE codigo_item_catalogo = %s AND id_compra = %s AND ni_fornecedor = %s""", 
                            dados_update
                        )
                        
                        rows_affected = cur.rowcount
                        total_atualizados += rows_affected
                        logging.info(f"Item {codigo_item}: {rows_affected} registros atualizados com sucesso")
                        
                    except Exception as e:
                        logging.error(f"Item {codigo_item}: Erro na atualização: {e}")
                        raise
            
            conn.commit()
            logging.info(f"Item {codigo_item}: Transação commitada com sucesso")
            
        except Exception as e:
            logging.error(f"Erro de DB para item {resultado.get('codigo', 'N/A')}: {e}", exc_info=True)
            total_erros_db += 1
            if conn:
                try:
                    conn.rollback()
                    logging.info(f"Item {resultado.get('codigo', 'N/A')}: Rollback executado")
                except:
                    pass
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    logging.info(f"Lote processado: {total_novos} novos, {total_atualizados} atualizados, {total_erros_db} erros")
    return total_novos, total_atualizados, total_erros_db

# --- Funções de Coleta ---

async def fetch_precos_item(session: aiohttp.ClientSession, codigo_item: str, tipo_item: str, ultima_data: Optional[datetime]) -> Dict:
    """Busca preços de um item específico na API"""
    tipo_normalizado = tipo_item.strip().lower()
    
    # Determinar URL baseada no tipo
    if 'material' in tipo_normalizado:
        base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"
    else:
        base_url = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco/3.1_consultarServico_CSV"
    
    params = {
        'codigoItemCatalogo': codigo_item,
        'pagina': 1,
        'tamanhoPagina': 500
    }
    
    # Configurar data de início se houver última sincronização
    if ultima_data:
        data_inicio_busca = ultima_data + timedelta(days=1)
        params['dataInicio'] = data_inicio_busca.strftime('%d/%m/%Y')
        logging.info(f"Buscando item {codigo_item} (a partir de {params['dataInicio']})...")
    else:
        logging.info(f"Buscando item {codigo_item} (histórico completo)...")

    all_dfs = []
    current_page = 1
    retries = 0
    sucesso_coleta = True
    total_registros = 0
    
    while True:
        params['pagina'] = current_page
        
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT)
            headers = {
                'accept': 'text/csv',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with session.get(base_url, params=params, headers=headers, timeout=timeout) as response:
                # Tratamento de rate limiting
                if response.status == 429:
                    if retries < MAX_RETRIES:
                        wait_time = 5 * (2 ** retries)
                        logging.warning(f"Item {codigo_item}: Rate limit (429) na página {current_page}. Tentativa {retries+1}/{MAX_RETRIES}. Aguardando {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        logging.error(f"Item {codigo_item}: Rate limit excedido após {MAX_RETRIES} tentativas. Desistindo deste item.")
                        sucesso_coleta = False
                        break
                
                # Outros erros HTTP
                if response.status != 200:
                    logging.warning(f"Item {codigo_item}: Status HTTP {response.status} na página {current_page}. Finalizando coleta.")
                    break
                
                # Reset retries em caso de sucesso
                retries = 0
                
                # Ler conteúdo
                content_bytes = await response.read()
                if not content_bytes:
                    logging.info(f"Item {codigo_item}: Conteúdo vazio na página {current_page}. Finalizando coleta.")
                    break
                
                # Processar CSV
                try:
                    cleaned_csv_text = _decode_and_clean_csv(content_bytes)
                    lines = cleaned_csv_text.strip().splitlines()
                    
                    if len(lines) < 2:  # Apenas cabeçalho ou vazio
                        logging.info(f"Item {codigo_item}: Sem dados na página {current_page}. Finalizando coleta.")
                        break
                    
                    # Criar DataFrame
                    df_page = pd.read_csv(
                        io.StringIO(cleaned_csv_text), 
                        sep=';', 
                        on_bad_lines='warn', 
                        engine='python', 
                        dtype=str
                    )
                    
                    if df_page.empty:
                        logging.info(f"Item {codigo_item}: DataFrame vazio na página {current_page}. Finalizando coleta.")
                        break
                    
                    # Normalizar nomes das colunas
                    df_page.columns = [normalizar_nome_coluna(col) for col in df_page.columns]
                    
                    # Substituir NaN por None
                    df_page = df_page.where(pd.notna(df_page), None)
                    
                    all_dfs.append(df_page)
                    total_registros += len(df_page)
                    
                    logging.debug(f"Item {codigo_item}: Página {current_page} processada com {len(df_page)} registros")
                    
                    current_page += 1
                    
                    # Rate limiting preventivo
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logging.error(f"Item {codigo_item}: Erro ao processar CSV da página {current_page}: {e}")
                    sucesso_coleta = False
                    break
                    
        except asyncio.TimeoutError:
            logging.error(f"Item {codigo_item}: Timeout na página {current_page}")
            sucesso_coleta = False
            break
        except Exception as e:
            logging.error(f"Item {codigo_item}: Erro de rede/processamento na página {current_page}: {e}")
            sucesso_coleta = False
            break
    
    # Processar dados coletados
    precos_coletados = []
    if sucesso_coleta and all_dfs:
        try:
            # Concatenar todos os DataFrames
            full_df = pd.concat(all_dfs, ignore_index=True)
            
            # Converter para lista de dicionários
            precos_coletados = full_df.to_dict('records')
            
            logging.info(f"SUCESSO: Item {codigo_item} - {len(precos_coletados)} preços coletados de {current_page-1} páginas")
            
        except Exception as e:
            logging.error(f"Item {codigo_item}: Erro ao concatenar/processar DataFrames: {e}")
            sucesso_coleta = False

    return {
        'codigo': codigo_item,
        'tipo': tipo_item,
        'precos': precos_coletados,
        'sucesso': sucesso_coleta,
        'total_registros': total_registros,
        'total_paginas': current_page - 1
    }

# --- Orquestração Principal ---

async def processar_e_salvar_lotes(session: aiohttp.ClientSession, itens_para_processar_lista: List[Tuple[str, Dict]], stats: Dict) -> List[Tuple[str, Dict]]:
    """Processa e salva lotes de itens"""
    itens_falhados = []
    total_lotes = (len(itens_para_processar_lista) + LOTE_SIZE - 1) // LOTE_SIZE
    
    for i in range(0, len(itens_para_processar_lista), LOTE_SIZE):
        lote_atual = itens_para_processar_lista[i:i + LOTE_SIZE]
        numero_lote = i // LOTE_SIZE + 1
        
        logging.info(f"\n{'='*20} PROCESSANDO LOTE {numero_lote}/{total_lotes} {'='*20}")
        logging.info(f"Itens no lote: {[codigo for codigo, _ in lote_atual]}")

        # Criar tasks para coleta assíncrona
        tasks = []
        for codigo, detalhes in lote_atual:
            task = asyncio.create_task(
                fetch_precos_item(session, codigo, detalhes['tipo'], detalhes['ultima_data']),
                name=f"Item-{codigo}"
            )
            tasks.append(task)
        
        # Aguardar conclusão das tasks com timeout
        try:
            done, pending = await asyncio.wait(tasks, timeout=TIMEOUT_LOTE)
            
            # Processar resultados concluídos
            resultados_coleta = []
            for task in done:
                try:
                    resultado = task.result()
                    resultados_coleta.append(resultado)
                    
                    if resultado['sucesso']:
                        logging.info(f"✅ Item {resultado['codigo']}: {resultado['total_registros']} registros coletados")
                    else:
                        logging.warning(f"❌ Item {resultado['codigo']}: Falha na coleta")
                        
                except Exception as e:
                    logging.error(f"Erro ao obter resultado da tarefa {task.get_name()}: {e}")

            # Cancelar tasks pendentes
            if pending:
                logging.warning(f"⏰ {len(pending)} tarefa(s) não concluída(s) dentro do timeout de {TIMEOUT_LOTE}s")
                for task in pending:
                    logging.warning(f"  - Tarefa pendente: {task.get_name()}")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            # Identificar itens que falharam na coleta
            codigos_sucesso_coleta = {res['codigo'] for res in resultados_coleta if res['sucesso']}
            for codigo, detalhes in lote_atual:
                if codigo not in codigos_sucesso_coleta:
                    itens_falhados.append((codigo, detalhes))

            # Filtrar apenas resultados válidos para sincronização
            resultados_validos = [res for res in resultados_coleta if res and res['sucesso'] and res['precos']]
            
            logging.info(f"Lote {numero_lote}: {len(resultados_validos)} itens com dados