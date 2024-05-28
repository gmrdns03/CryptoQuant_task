from airflow import DAG
from datetime import datetime, timedelta

# airflow
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable

# custom operators
from operators.StatusOperator import StatusOperator

# libs
import pandas as pd
import time
import json

LIVECOINWATCH_API_KEY = Variable.get("livecoinwatch-api-key")
SQL_FOLDER_PATH = Variable.get("sql_folder_path")
FILE_FOLDER_PATH = Variable.get("file_folder")

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# 수집 요청 테이블 확인
def _req_chk(conn_id, task_instance) :
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    post_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        select col_req_no, col_str_dt, col_end_dt, token_cd from collect_req
        where col_stat = 'N' and col_id = 'onchain' order by col_req_no limit 1
    """
    req_record = post_hook.get_first(sql)
    if len(req_record) > 0 :
        col_req_no = req_record[0]
        token_cd = req_record[3]
        get_addr_sql = f"select token_address from token_address where token_cd = '{token_cd}'"
        token_address = post_hook.get_first(get_addr_sql)[0]
        col_str_dt = req_record[1]
        col_end_dt = req_record[2]
        task_instance.xcom_push(key="col_req_no", value=col_req_no)
        task_instance.xcom_push(key="token_cd", value=token_cd)
        task_instance.xcom_push(key="token_address", value=token_address)
        task_instance.xcom_push(key="col_str_dt", value=col_str_dt)
        task_instance.xcom_push(key="col_end_dt", value=col_end_dt)
        return "etl_test"

# 데이터 수집 및 데이터베이스에 적재
def _onchain_file_etl(token_address, col_str_dt, col_end_dt, **kwargs) :
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    path = FILE_FOLDER_PATH + "/cryptoquant_take_home_assignment_data_engineer.csv"
    src_df = pd.read_csv(path)
    src_df["block_timestamp"] = src_df["block_timestamp"].map(lambda x : x.rstrip(" UTC"))
    str_dt_utc = datetime.strptime(col_str_dt, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    end_dt_utc = datetime.strptime(col_end_dt, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    str_dt = datetime.strftime(str_dt_utc, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strftime(end_dt_utc, "%Y-%m-%d %H:%M:%S")
    trg_df = src_df[src_df["block_timestamp"].between(str_dt, end_dt)]
    trg_df = trg_df[trg_df["token_address"] == token_address]
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    trg_df.to_sql("onchain_trans_log", con=postgres_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
    return True

with DAG (
    'collect_onchain_history_data',
    default_args=default_args,
    description='온체인 히스토리 수집을 위한 DAG',
    schedule_interval='@once',
    start_date=datetime(2024, 5, 25),
    catchup=False,
    tags=['test']
) as dag :
    req_chk = BranchPythonOperator(
        task_id="req_chk",
        python_callable=_req_chk,
        op_args=["postgres"]
    )

    etl_test = PythonOperator(
        task_id="etl_test",
        python_callable=_onchain_file_etl,
        op_kwargs={
            "token_address" : "{{ti.xcom_pull(key='token_address', task_ids='req_chk')}}",
            "col_str_dt": "{{ti.xcom_pull(key='col_str_dt', task_ids='req_chk')}}",
            "col_end_dt": "{{ti.xcom_pull(key='col_end_dt', task_ids='req_chk')}}"
        }
    )

    req_update = StatusOperator(
        task_id="req_update",
        conn_id="postgres",
        xcom_key="col_req_no",
        xcom_task_ids="req_chk",
        status="Y"
    )

    err_handling = StatusOperator(
        task_id="err_handling",
        conn_id="postgres",
        xcom_key="col_req_no",
        xcom_task_ids="req_chk",
        status="E",
        trigger_rule="one_failed"
    )



    req_chk >> etl_test >> [req_update, err_handling]
