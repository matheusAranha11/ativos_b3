from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from augme_utils.credentials.credentials import default_directory
import pendulum

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


br_timezone = pendulum.timezone("Brazil/East")

with DAG(

    'atualizar_isins_b3_dag',

    start_date = pendulum.datetime(2023, 4, 11, tz=br_timezone),

    schedule_interval = '0 7 2 * *',

    tags = ["Cadastro"],

    catchup = False

) as dag:

    update_utils = BashOperator(task_id='update_utils', bash_command='pip install git+https://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@github.com/vanadio-augme/augme_utils.git --force-reinstall')
    download_isins = PythonOperator(task_id='download_atualizacoes', python_callable = download_atualizacoes_isins_b3)
    unzip_arq = PythonOperator(task_id='unzip_arq', python_callable = unzip_isins_b3)
    etl_isins = PythonOperator(task_id='elt_isins', python_callable = etl_b3_txt)

    #update_utils >> download_isins >> unzip_arq >> etl_isins

    download_isins