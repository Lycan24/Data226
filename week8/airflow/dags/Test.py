# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
import requests
from datetime import datetime, timedelta



def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def creat_table():
    cor = return_snowflake_conn()
    sql1 = '''
    CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
    );'''

    sql2 = '''CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp  
    );'''


    try:  
        cor.execute("BEGIN;")
        cor.execute(sql1)
        cor.execute(sql2)
        cor.execute("COMMIT;")
    
    except Exception as e:
        cor.execute("ROLLBACK;")
        print(e)
        raise e
@task
def populate_table():
    cor = return_snowflake_conn()

    sql3 = '''CREATE OR REPLACE STAGE dev.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');'''

    sql4 = '''COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv;'''

    sql5 = '''COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv;'''
    try:  
        cor.execute("BEGIN;")
        cor.execute(sql3)
        cor.execute(sql4)
        cor.execute(sql5)
        cor.execute("COMMIT;")
    
    except Exception as e:
        cor.execute("ROLLBACK;")
        print(e)
        raise e
        
        
with DAG(
    dag_id = 'SessionToSnowflake',
    start_date = datetime(2024,10,19),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    creat_table()
    populate_table()
   
