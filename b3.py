import pandas as pd
from augme_utils.connections.connections import create_database_connection



def load_b3_txt(txt_filepath):

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

    """
    CONVERTER COLUNAS PARA DATE
        data_expiracao
        data_geracao
        data_emissao
        data_expiracao
        data_acao
        data_base
        data_primeirop_pagamento_juros
        
    """

    
    ##########################

    b3_table = 'ativos_b3'
    vanadio_connection = create_database_connection("VANADIO")

    print(f"Subindo {len(b3_df)} na tabela {b3_table} do banco VANADIO...\n")
    b3_df.to_sql(name=b3_table, schema='bronze', con=vanadio_connection, index=False, if_exists='append')
    print("Concluído.\n")

    vanadio_connection.close()
    return


    # definir tabela pra subir
    # subir pra tabela

filepath = 'C:/Users/matheus.lopes/Documents/github/ativos_b3/docs/total/NUMERACA.TXT'
b3 = load_b3_txt(filepath)

print("a")