from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import boto3, re, os
import vertica_python
from getpass import getpass
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'email': ['ruslan.salyakhov@outlook.com'],
    'email_on_failure': False,
    'max_active_runs': 1,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

session = boto3.session.Session()

def get_s3_data(session):
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Initiate an empty file list 
    file_list = []
    
    # Checking if download folder doesn't exist creating it 
    path = '/lessons/data'
    if not os.path.exists(path):
        os.mkdir(path)
    
    # Get a list of files have been already downloaded to directory
    files = os.listdir('/lessons/data/')
    
    # Get a list of objects from the bucket
    objects = s3_client.list_objects(Bucket='final-project')
    
    # If file was already put into directory we skip it
    for object in objects['Contents']:
        file_name = object['Key']
        
        if file_name in files:
            continue
            
        else:
            file_list.append(file_name)
            s3_client.download_file(
                Bucket='final-project',
                Key=str(file_name),
                Filename=f'/lessons/data/{file_name}')
                
    return file_list
    
def order(line):
    if len(re.findall("\d+", line)) == 0:
        return -1
    else:
        return int(re.findall("\d+", line)[0])

conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': 'ruslan_saliahoff_yandex_ru',       
             'password': '********', 
             # 'password': getpass(),
             'database': 'dwh',
             # Use autocommit
             'autocommit': True
}

def copy_to_dwh(ti, conn_info=conn_info, dir='/lessons/data/'):
    
    # Getting values returned by previous task to copy to Vertica only files which were downloaded by previous task from S3 
    files = ti.xcom_pull(task_ids=['get_data_from_s3'])
    files = files[0]
    
    # Sort files by sort method using order function
    files.sort(key=order)

    # Open connection to Vertica
    with vertica_python.connect(**conn_info) as conn:

        # Open a cursor to perform database operations
        cur = conn.cursor()
        
        # Checking list of files returned by previous task
        if len(files) == 0:
            return 1 
          
        else:
            # Since we cannot iterate over list with single element append additional value as placeholder
            if len(files) == 1:
                files.append("placeholder")
                
            # Iterate over files list
            for file in files:
            
                if 'currencies' in str(file):
                    #If we already copied this file data to Vertica using try and except to handle this error
                    try:
                        cur.execute(f"COPY RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies(currency_code, currency_code_with, date_update, currency_code_div) FROM LOCAL '/lessons/data/{file}' DELIMITER ',';")
                    except:
                        print("Something went wrong during COPY operation to RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies !")
                        
                elif 'batch' in str(file):
                    try:
                        cur.execute(f"COPY RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions(operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt) FROM LOCAL '/lessons/data/{file}' DELIMITER ',';")
                    except:
                        print("Something went wrong during COPY operation to RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions !")
        return files
         # Query the database and obtain data
        #res = cur.fetchall()
        #return res
        
        
def update_mart(conn_info=conn_info, sdate='2022-10-01', edate = '2022-10-31'):
    
    temp_view = """CREATE OR REPLACE VIEW RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions_dollars AS
                (SELECT t.operation_id, t.account_number_from, t.account_number_to, t.currency_code,
                        t.country, t.status, t.transaction_type, t.amount, t.transaction_dt,
                        cur.date_update, cur.currency_code_with, cur.currency_code_div,
                        CASE 
                            WHEN (cur.currency_code = 420) THEN t.amount 
                            WHEN (cur.currency_code != 420) THEN (t.amount / cur.currency_code_div) 
                        END AS amount_dollars       
                FROM RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions t 
                LEFT JOIN RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies cur 
                ON (t.currency_code = cur.currency_code AND t.transaction_dt::date = cur.date_update::date)
                WHERE ((cur.currency_code = 420 AND cur.currency_code_with = 410 AND t.account_number_from != -1) 
                    OR (cur.currency_code_with = 420 AND t.account_number_from != -1)) AND t.status = 'done');"""
    
    # Merge command template
    merge="""MERGE INTO RUSLAN_SALIAHOFF_YANDEX_RU__DWH.global_metrics  AS tgt
            USING 
                (SELECT date_update::date, 420 AS currency_from, SUM(amount_dollars) AS amount_total, 
                    COUNT(1) AS cnt_transactions, COUNT(account_number_to)/COUNT(DISTINCT account_number_to) AS avg_transactions_per_account, 
                    COUNT(DISTINCT account_number_to) AS cnt_accounts_make_transactions
                FROM RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions_dollars  
                WHERE date_update::date = '{0}' GROUP BY date_update) src
            ON tgt.date_update = src.date_update  
            WHEN NOT MATCHED
            THEN INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
            VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);"""
    # Open connection to Vertica
    with vertica_python.connect(**conn_info) as conn:

        # Open a cursor to perform database operations
        cur = conn.cursor()
        
        # Get a list dates when we had transactions operations 
        cur.execute('SELECT DISTINCT DATE(transaction_dt) from RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions ORDER BY DATE(transaction_dt);')
        rows = cur.fetchall()
        
        # Convert a list of values datetime.date(2022, 10, 1) type to the list of strings like 2022-10-01 
        tr_dates = [i[0].strftime('%Y-%m-%d') for i in rows]
         
        # Create a temp view we are going to use for data mart update
        cur.execute(temp_view)
        
        iterdate = []
        
        # Define a list we are going to use as a return value
        re_data = []
        
        if (str(sdate) == str(edate)) or len(edate) == 0:
            # If there is only one item in the list in order to iterate over it add date '2000-10-01' as a placeholder.
            iterdate = [sdate]
            iterdate.append('2000-10-01')
        
        else:
            # Get a list with range of dates from sdate up to and including edate
            iterdate = [i.strftime('%Y-%m-%d') for i in pd.date_range(sdate, edate).tolist()]
       
        
        # Iterate over files list
        for data in iterdate:
            
            
            # If data from defined range is included in the transaction dates to update data mart with information for this date 
            if data in tr_dates:
                
                re_data.append(data)
                
                merge_format = merge.format(str(data))
            #If we already copied this file data to Vertica using try and except to handle this error
                try:
                    cur.execute(merge_format)
                except:
                    print("Something went wrong during MERGE operation INTO RUSLAN_SALIAHOFF_YANDEX_RU__DWH.global_metrics !")
                    
            else:
                continue
        
    return re_data
  

with DAG(dag_id="final_project", schedule_interval="0 * * * *", default_args=default_args, catchup=False) as dag:

  

    download_from_s3 = PythonOperator(
        task_id='get_data_from_s3',
        python_callable=get_s3_data,
        op_kwargs={
            'session': session
        }
    )
    
    copy_to_vertica = PythonOperator(
        task_id='copy_data_to_vertica',
        python_callable=copy_to_dwh,
        op_kwargs={
            'conn_info': conn_info,
            'dir': '/lessons/data/'
        }
    )
    
    update_datamart = PythonOperator(
        task_id='update_data_to_mart',
        python_callable=update_mart,
        op_kwargs={
            'conn_info': conn_info,
            'sdate': '2022-10-01',
            'edate': '2022-10-31'
        }
    )

download_from_s3 >> copy_to_vertica >> update_datamart


