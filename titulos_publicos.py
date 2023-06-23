
from augme_utils.anbima.anbima_feed import AnbimaTPF, AnbimaConnect, CLIENT_ID, CLIENT_SECRET
from augme_utils.connections.connections import create_database_connection
import pandas as pd
import datetime
import requests


def get_titulos_anbima(target_date):

    if isinstance(target_date,str):
        target_date = datetime.datetime.strptime(target_date,'%Y-%m-%d').date()

    con = AnbimaConnect(CLIENT_ID, CLIENT_SECRET)
    tpf = AnbimaTPF(con, ambiente='PRODUCTION')
    response = tpf.mercado_secundario(target_date)    

    titulos = response.json()
    titulos_df = pd.json_normalize(titulos)

    return titulos_df

def get_titulos_cadastrados():

    cnxn = create_database_connection("PREGO")
    query_titulos = """
        SELECT isin
        FROM prego.prego.ativos_ativo
        JOIN prego.prego.ativos_titulopublico
        ON ativos_ativo.id = ativos_titulopublico.rendafixa_ptr_id
    """

    titulos_cadastrados = pd.read_sql_query(query_titulos, cnxn)
    cnxn.close()
    return titulos_cadastrados



def tratamento_novos_titulos(novos_titulos):

    # Renomear e dropar colunas
    novos_titulos.drop(columns=[
                                'expressao',
                                'data_referencia',
                                'taxa_compra',
                                'taxa_venda',
                                'taxa_indicativa',
                                'intervalo_min_d0',
                                'intervalo_max_d0',
                                'intervalo_max_d1',
                                'intervalo_min_d1',
                                'pu',
                                'desvio_padrao'
                            ], inplace=True)

    novos_titulos.rename(columns={
                                'data_vencimento':'vencimento',
                                'codigo_selic':'trading_code',
                                'data_base':'data_emissao',
                                'codigo_isin':'isin'
                                }, inplace=True)

    # Eliminar as linhas com valores 'NTN-C' e 'NTN-F' na coluna 'tipo_titulo'
    novos_titulos = novos_titulos[~novos_titulos['tipo_titulo'].isin(['NTN-C', 'NTN-F'])]

    # Mapeamento de indexadores e taxas de emissão
    map_indexador = {'LFT': 'CDI%', 'NTN-B': 'IPCA+', 'LTN':'PRE'}
    map_taxa_emissao = {'LFT': 1, 'NTN-B': -1, 'LTN':-1}

    # Adicionar as colunas "indexador" e "taxa_emissao" ao DataFrame original
    novos_titulos['indexador'] = novos_titulos['tipo_titulo'].map(map_indexador)
    novos_titulos['taxa_emissao'] = novos_titulos['tipo_titulo'].map(map_taxa_emissao)

    # Adicionar colunas para o cadastro
    novos_titulos['Risco'] = 'TBD'
    novos_titulos['Senioridade'] = 'TBD'
    novos_titulos['garantia'] = 'TBD'
    novos_titulos['liquidez_esperada'] = 'TBD'
    novos_titulos['tipo_divida'] = 'TBD'
    novos_titulos['Moeda'] = 'BRL'
    novos_titulos['data_inicio_rentabilidade'] = novos_titulos['data_emissao']
    novos_titulos['Book'] = 'Caixa'
    novos_titulos['artigo_emissao'] = 'TBD'
    novos_titulos['setor_industry_group'] = 'TBD'
    novos_titulos['emissor'] = 436
    novos_titulos['emissor_risco'] = 436
    novos_titulos['grupo_economico'] = 245
    novos_titulos['analista_de_gestao'] = 1
    novos_titulos['trade'] = 409
    novos_titulos['subclasse'] = 2
    novos_titulos['credit_score'] = 1

    # Ajustando o nome dos ativos
    vencimento_string = novos_titulos['vencimento']
    vencimento = pd.to_datetime(vencimento_string)
    vencimento_formatado = vencimento.dt.strftime("%m/%Y")
    novos_titulos['nome_ativo'] = novos_titulos['tipo_titulo'].str.cat(vencimento_formatado, sep = ' ')

    return novos_titulos



def etl_novos_titulos(target_date):
    titulos_anbima = get_titulos_anbima(target_date)
    titulos_cadastrados = get_titulos_cadastrados()

    novos_titulos = titulos_anbima[~titulos_anbima['codigo_isin'].isin(titulos_cadastrados)]
    

    novos_titulos_tratados = tratamento_novos_titulos(novos_titulos)
    
    # POST REQUESTS
    
    if len(novos_titulos) > 0:

        titulos_url = 'https://vanadio.azurewebsites.net/ativos/rest/titpublico/'
        novos_titulos_dict = novos_titulos_tratados.to_dict(orient='records')

        for titulo in novos_titulos_dict:
            response = requests.post(url=titulos_url, json=titulo)

            if response.status_code == 201:
                print(f'Ativo {titulo["nome_ativo"]} cadastrado com sucesso.')
            else:
                print('Falha na requisição')

    else:
        print("Nao há nenhum novo titulo publico a ser cadastrado.")


    return    

