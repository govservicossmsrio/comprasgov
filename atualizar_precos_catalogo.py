def sync_lote_precos_catalogo(conn_string: str, resultados_lote: List[Dict]) -> Tuple[int, int, int]:
    total_novos, total_atualizados, total_erros_db = 0, 0, 0
    
    for resultado in resultados_lote:
        conn = None
        try:
            codigo_item = resultado['codigo']
            tipo_item = resultado['tipo']
            precos_api = resultado['precos']

            if not precos_api:
                continue

            conn = psycopg2.connect(conn_string)
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM precos_catalogo WHERE codigo_item_catalogo = %s::VARCHAR", (codigo_item,))
                precos_db_map = {f"{row['id_compra']}-{row['ni_fornecedor']}": row for row in cur}
                
                precos_para_inserir, precos_para_atualizar = [], []
                
                for p in precos_api:
                    id_compra_api = p.get('numero_compra', '')
                    ni_fornecedor_api = p.get('cnpj_vencedor', '')
                    
                    if not id_compra_api or not ni_fornecedor_api:
                        continue
                    
                    chave_api = f"{id_compra_api}-{ni_fornecedor_api}"
                    
                    # Conversão segura do valor da API
                    try:
                        valor_unitario_api = float(p.get('valor_unitario_homologado', 0))
                    except (ValueError, TypeError):
                        logging.warning(f"Valor inválido na API para item {codigo_item}: {p.get('valor_unitario_homologado')}")
                        continue
                    
                    preco_existente = precos_db_map.get(chave_api)
                    
                    if not preco_existente:
                        precos_para_inserir.append(p)
                    else:
                        # CORREÇÃO CRÍTICA: Comparação adequada de valores
                        try:
                            valor_db = float(preco_existente['valor_unitario']) if preco_existente['valor_unitario'] is not None else 0.0
                            
                            # Comparação com tolerância de 1 centavo
                            if abs(valor_unitario_api - valor_db) > 0.01:
                                precos_para_atualizar.append(p)
                                logging.debug(f"Diferença detectada para {chave_api}: API={valor_unitario_api}, DB={valor_db}")
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Erro ao comparar valores para {chave_api}: {e}")
                            continue
                
                # Inserções
                if precos_para_inserir:
                    dados_insert = [
                        (codigo_item, tipo_item, p.get('descricao_item_catalogo'), 
                         p.get('unidade_fornecimento'), p.get('quantidade_item'), 
                         p.get('valor_unitario_homologado'), p.get('valor_total_homologado'), 
                         p.get('cnpj_vencedor'), p.get('nome_vencedor'), 
                         p.get('numero_compra'), p.get('data_resultado'), datetime.now()) 
                        for p in precos_para_inserir
                    ]
                    psycopg2.extras.execute_values(
                        cur, 
                        "INSERT INTO precos_catalogo (codigo_item_catalogo, tipo_item, descricao_item, unidade_medida, quantidade_total, valor_unitario, valor_total, ni_fornecedor, nome_fornecedor, id_compra, data_compra, data_atualizacao) VALUES %s", 
                        dados_insert
                    )
                    total_novos += len(dados_insert)
                    logging.info(f"✅ {len(dados_insert)} novos preços inseridos para item {codigo_item}")

                # Atualizações
                if precos_para_atualizar:
                    logging.info(f"Preparando atualização de {len(precos_para_atualizar)} preços para item {codigo_item}")
                    
                    dados_update = [
                        (p.get('descricao_item_catalogo'), p.get('unidade_fornecimento'), 
                         p.get('quantidade_item'), p.get('valor_unitario_homologado'), 
                         p.get('valor_total_homologado'), p.get('nome_fornecedor'), 
                         p.get('data_resultado'), codigo_item, 
                         p.get('numero_compra'), p.get('cnpj_vencedor')) 
                        for p in precos_para_atualizar
                    ]
                    
                    psycopg2.extras.execute_batch(
                        cur, 
                        """UPDATE precos_catalogo 
                           SET descricao_item = %s, unidade_medida = %s, quantidade_total = %s, 
                               valor_unitario = %s, valor_total = %s, nome_fornecedor = %s, 
                               data_compra = %s, data_atualizacao = CURRENT_TIMESTAMP 
                           WHERE codigo_item_catalogo = %s AND id_compra = %s AND ni_fornecedor = %s""", 
                        dados_update
                    )
                    
                    linhas_atualizadas = cur.rowcount
                    total_atualizados += linhas_atualizadas
                    logging.info(f"✅ {linhas_atualizadas} linhas efetivamente atualizadas no banco para item {codigo_item}")
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Erro de DB para item {resultado.get('codigo', 'N/A')}: {e}", exc_info=True)
            total_erros_db += 1
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
    
    if total_novos > 0 or total_atualizados > 0:
        logging.info(f"Lote salvo com sucesso no banco: {total_novos} novos, {total_atualizados} atualizados.")
            
    return total_novos, total_atualizados, total_erros_db