import os
import requests
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
import pandas as pd
import io

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv(dotenv_path='dbconnection.env')

CONN_STRING = os.getenv('COCKROACHDB_CONN_STRING')
API_BASE_URL = "https://dadosabertos.compras.gov.br/modulo-pesquisa-preco"

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

def get_itens_catalogo_from_db(conn):
    """Busca todos os itens de catálogo distintos do banco."""
    with conn.cursor() as cur:
        # A query junta com itens_compra para pegar o tipo 'material' ou 'serviço'
        # que pode não estar na tabela itens_catalogo ainda.
        cur.execute("""
            SELECT DISTINCT
                ic.codigo,
                COALESCE(ic.tipo, icomp.materialOuServicoNome) as tipo
            FROM itens_catalogo ic
            LEFT JOIN (
                SELECT DISTINCT codItemCatalogo, materialOuServicoNome 
                FROM itens_compra_raw_pncp -- Supondo uma tabela temporária ou a final
            ) icomp ON ic.codigo = icomp.codItemCatalogo
            WHERE ic.codigo IS NOT NULL;
        """)
        return cur.fetchall()

# --- Funções de API e Tratamento ---

def fetch_and_treat_csv(endpoint, codigo_item):
    """Busca o CSV da API, trata os caracteres e retorna um DataFrame."""
    params = {'codigoItemCatalogo': codigo_item, 'pagina': 1, 'tamanhoPagina': 1000} # Aumentar tamanhoPagina
    url = f"{API_BASE_URL}/{endpoint}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Tratamento de codificação
        content = response.content
        try:
            # Tenta decodificar com 'latin-1', comum em sistemas governamentais
            decoded_content = content.decode('latin-1')
        except UnicodeDecodeError:
            # Se falhar, tenta 'cp1252', outra opção comum
            decoded_content = content.decode('cp1252')

        # Substitui os caracteres problemáticos
        # Esta é uma forma de normalizar. Pode ser ajustada conforme necessário.
        # Ex: "PREGÃƒO" -> "PREGÃO"
        # A decodificação correta é o mais importante.
        # A normalização abaixo é um paliativo se a decodificação não for perfeita.
        # decoded_content = decoded_content.replace('Ã‡', 'Ç').replace('Ã£', 'ã')...
            
        # Usa o pandas para ler o CSV a partir do texto decodificado
        df = pd.read_csv(io.StringIO(decoded_content), sep=';')
        return df
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar CSV de {endpoint} para item {codigo_item}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro ao processar CSV para item {codigo_item}: {e}")
        return None

# --- Função Principal ---

def main():
    """Função principal que orquestra o processo."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        itens_para_buscar = get_itens_catalogo_from_db(conn)
        logging.info(f"Encontrados {len(itens_para_buscar)} itens de catálogo para verificar.")
        
        for codigo, tipo in itens_para_buscar:
            if not tipo:
                logging.warning(f"Item {codigo} sem tipo definido. Pulando.")
                continue

            logging.info(f"Processando item {codigo} do tipo '{tipo}'...")
            
            endpoint = ""
            if 'material' in tipo.lower():
                endpoint = "1.1_consultarMaterial_CSV"
            elif 'serviço' in tipo.lower():
                endpoint = "3.1_consultarServico_CSV"
            else:
                logging.warning(f"Tipo '{tipo}' não reconhecido para o item {codigo}. Pulando.")
                continue
            
            df_precos = fetch_and_treat_csv(endpoint, codigo)
            
            if df_precos is not None and not df_precos.empty:
                logging.info(f"Encontrados {len(df_precos)} registros de preço para o item {codigo}.")
                # Aqui viria a lógica para fazer o UPSERT desses dados
                # Você precisaria mapear as colunas do DataFrame para as tabelas do banco
                # (compras, itens_compra, resultados_itens, fornecedores, etc.)
                # e usar a função upsert_data.
                # Ex:
                # dados_para_resultados = df_precos[['idItemCompra', 'niFornecedor', ...]].values.tolist()
                # upsert_data(conn, 'resultados_itens', colunas, dados_para_resultados, ...)
            else:
                logging.info(f"Nenhum registro de preço encontrado para o item {codigo}.")

    finally:
        conn.close()
        logging.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()
