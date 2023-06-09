import pandas as pd
from augme_utils.connections.connections import create_database_connection


def etl_b3_txt(txt_filepath):
    """
    Rotina que le o arquivo de isins da b3 e sobe os dados para o banco

    Args:
        txt_filepath(str): caminho do arquivo contendo as informacoes
    """

    # LEITURA DO ARQUIVO
    b3_df = pd.read_csv(txt_filepath)
    columns = [
        'data_geracao', 'acao_sofrida', 'isin',
        'codigo_emissor', 'codigo_cfi',
        'descricao', 'ano_emissao', 'data_emissao',
        'ano_expiracao', 'data_expiracao', 'taxa_juros', 'moeda', 'valor_nominal',
        'preco_exercicio', 'indexador', 'percentual_indexador',
        'data_acao', 'codigo_cetip', 'codigo_selic', 'codigo_pais',
        'tipo_ativo', 'codigo_categoria', 'codigo_especie', 
        'data_base', 'numero_emissao', 'numero_de_serie',
        'tipo_emissao', 'tipo_ativo_objeto', 'tipo_entrega',
        'tipo_fundo', 'tipo_garantia', 'tipo_juros',
        'tipo_mercado', 'status_isin', 'tipo_vencimento', 
        'tipo_protecao', 'tipo_politica_distribuicao_fundos',
        'tipo_politica_investimento_fundo', 'tipo_forma', 
        'tipo_estilo_opcao', 'numero_serie_opcao', 'codigo_frequencia_juros',
        'situacao_isin', 'data_primeirop_pagamento_juros'
        ]
    b3_df.columns = columns
    
    # TRATAMENTO PARA DATAS
    date_columns = ['data_expiracao', 'data_geracao', 'data_emissao', 'data_expiracao', 'data_acao', 'data_base', 'data_primeirop_pagamento_juros']

    for column in date_columns:
    
        b3_df[column] = pd.to_datetime(b3_df[column], format='%Y%m%d', errors='coerce').dt.date
        
    
    b3_df['ano_emissao'] = b3_df['ano_emissao'].fillna(-1)
    b3_df['ano_emissao'] = b3_df['ano_emissao'].astype(int)
    b3_df['ano_emissao'] = b3_df['ano_emissao'].replace(-1, pd.NA)

    b3_df['ano_expiracao'] = b3_df['ano_expiracao'].fillna(-1)
    b3_df['ano_expiracao'] = b3_df['ano_expiracao'].astype(int)
    b3_df['ano_expiracao'] = b3_df['ano_expiracao'].replace(-1, pd.NA)

    # SUBINDO DADOS PARA O BANCO
    b3_table = 'ativos_b3'
    vanadio_connection = create_database_connection("VANADIO")

    print(f"Subindo {len(b3_df)} na tabela {b3_table} do banco VANADIO.../n")
    b3_df.to_sql(name=b3_table, schema='bronze', con=vanadio_connection, index=False, if_exists='append')
    print("Concluído./n")

    vanadio_connection.close()
    
    return None

