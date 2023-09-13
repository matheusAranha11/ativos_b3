from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pendulum




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
        FROM VANADIO.bronze.b3_isins
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



br_timezone = pendulum.timezone("Brazil/East")

with DAG(

    'cadastro_futuros',

    start_date = pendulum.datetime(2023, 8, 10, tz=br_timezone),

    schedule_interval = '0 7 2 * *',

    tags = ["Cadastro"],

    catchup = False

) as dag:

    
    cadastro_futuros = PythonOperator(task_id='cadastrar_futuros', python_callable = cadastrar_futuros)
    
    cadastro_futuros