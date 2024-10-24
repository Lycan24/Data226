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
def table_join():
    cor = return_snowflake_conn()

    sql = '''CREATE OR REPLACE TABLE dev.raw_data.session_summary as 
        select usc.SESSIONID, usc.USERID, usc.CHANNEL, st.TS
        from dev.raw_data.user_session_channel as usc
        full join dev.raw_data.session_timestamp as st
        on sessionId;'''

    try:  
        cor.execute("BEGIN;")
        cor.execute(sql)
        cor.execute("COMMIT;")
    
    except Exception as e:
        cor.execute("ROLLBACK;")
        print(e)
        raise e
@task
def duplicate_check():
    cor = return_snowflake_conn()


    sql = '''CREATE OR REPLACE TABLE duplicateCheck as 
        SELECT COUNT(DISTINCT SS.SESSIONID)
        FROM dev.raw_data.session_summary as SS
        ORDER BY SS.SESSIONID DESC 
        LIMIT 1;'''

    try:  
        cor.execute("BEGIN;")
        cor.execute(sql)
        cor.execute("COMMIT;")
    
    except Exception as e:
        cor.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'TableJoin',
    start_date = datetime(2024,10,19),
    catchup=False,
    tags=['ETL'],
    schedule = '40 2 * * *'
) as dag:
    table_join()
    duplicate_check()