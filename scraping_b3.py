# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from augme_utils.credentials.credentials import default_directory

# import pendulum

download_dir = default_directory + 'isins_b3'

def download_atualizacoes_isins_b3():

    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    from selenium.webdriver.common.action_chains import ActionChains
    from webdriver_manager.chrome import ChromeDriverManager
    import time
    
    print(download_dir)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")
    #chrome_options.add_argument(f"download.default_directory={download_dir}")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--safebrowsing-disable-download-protection")
    prefs = {'download.default_directory': download_dir}
    download_dir.add_experimental_option('prefs', prefs)
    
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    actions = ActionChains(driver)

    b3_url = 'https://sistemaswebb3-listados.b3.com.br/isinPage/#accordionBodyTwo'

    driver.get(b3_url)

    downloads = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(
        (By.XPATH, "/html/body/app-root/app-isin-home/div/form/div/div/div[1]/div[2]/div[1]/div/div/a")))
    actions.move_to_element(downloads).perform()
    downloads.click()
    time.sleep(5)

    
    atualizacoes_mensais = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(
        (By.XPATH, "//*[@id='accordionBodyTwo']/div/div[1]/div[3]/div[2]/p[1]/a")))
    actions.move_to_element(atualizacoes_mensais).perform()
    atualizacoes_mensais.click()
    time.sleep(30)

    driver.quit()
    driver.close()

    return None

def unzip_isins_b3():

    import zipfile
    # Get the directory path of the zip file
    zip_path = download_dir + '/ISINS_M.zip'

    # Open the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Extract all contents to the same directory
        zip_ref.extractall(download_dir)

    return None 

def etl_b3_txt():

    from augme_utils.connections.connections import create_database_connection
    import pandas as pd

    txt_filepath = download_dir + '/NUMERACAO.txt'

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

def clear_dir():
    import os

    for filename in os.listdir(download_dir):
        file_path = os.path.join(download_dir, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Deleted file: {file_path}")

def cadastrar_futuros():
    from augme_utils.connections.connections import create_database_connection
    import pandas as pd
    import requests

    query_futuros = """
    WITH codigos_futuros AS (
	SELECT *
	FROM VANADIO.bronze.FUTUROS_BUSCADOS

    ),

    futuros_cadastrados AS (

        SELECT isin,
            trading_code
        FROM PREGO.prego.ativos_futuros f
        JOIN PREGO.prego.ativos_derivativo d ON f.derivativo_ptr_id = d.ativo_ptr_id
        JOIN PREGO.prego.ativos_ativo a ON d.ativo_ptr_id = a.id
    ),

    futuros_no_arquivo AS (

        SELECT isin,
            CONCAT(tipo_ativo, '', numero_serie_opcao) trading_code,
            CONCAT('20', RIGHT(numero_serie_opcao, 2), '-', 
                CASE 
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'F' THEN '01-31'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'G' THEN '02-28'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'H' THEN '03-31'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'J' THEN '04-30'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'K' THEN '05-31'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'M' THEN '06-30'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'N' THEN '07-31'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'Q' THEN '08-31'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'U' THEN '09-30'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'V' THEN '10-31'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'X' THEN '11-30'
                    WHEN SUBSTRING(numero_serie_opcao, 1, 1) = 'Z' THEN '12-31'
                END) AS vencimento
        FROM VANADIO.bronze.ativos_b3
        WHERE descricao LIKE 'CONTRATO%FUTURO%'
        AND numero_serie_opcao IS NOT NULL
        AND tipo_ativo IN (SELECT *
                        FROM codigos_futuros)

    ),


    novos_futuros AS (

        SELECT futuros_no_arquivo.isin,
            futuros_no_arquivo.trading_code,
            nome_ativo = futuros_no_arquivo.trading_code,
            risco = 'TBD',
            senioridade = 'TBD',
            tipo_garantia = 'TBD',
            liquidez_esperada = 'TBD',
            futuros_no_arquivo.vencimento,
            moeda = 'BRL'
        FROM futuros_no_arquivo
        WHERE isin NOT IN (SELECT isin
                        FROM futuros_cadastrados)
        AND trading_code NOT IN (SELECT trading_code
                                FROM futuros_cadastrados)

    )

    SELECT *
    FROM novos_futuros
    """

    con = create_database_connection("VANADIO")
    novos_futuros = pd.read_sql_query(query_futuros, con)

    if len(novos_futuros) > 0:
        url_futuros = 'https://vanadio.azurewebsites.net/ativos/rest/futuros/'
        dict_futuros = novos_futuros.to_dict(orient='records')
        for futuro in dict_futuros:

            response = requests.post(url=url_futuros, json=futuro)

            if response.status_code == 201:
                print(f'Ativo {futuro["nome_ativo"]} cadastrado com sucesso.')
            else:
                print('Falha na requisição')

    else:
        print("Nao há nenhum novo futuro a ser cadastrado.")

    con.close()

    return None

cadastrar_futuros()