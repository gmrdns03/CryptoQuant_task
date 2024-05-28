from airflow import DAG
from datetime import datetime, timedelta

# airflow
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

# custom operators
from operators.StatusOperator import StatusOperator

# libs
import pandas as pd
import time
import json

LIVECOINWATCH_API_KEY = Variable.get("livecoinwatch-api-key")
FILE_FOLDER = Variable.get("sql_folder_path")

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
        where col_stat = 'N' and col_id = 'price' order by col_req_no limit 1
    """
    req_record = post_hook.get_first(sql)
    if len(req_record) > 0 :
        col_req_no = req_record[0]
        token_cd = req_record[3]
        get_addr_sql = f"select token_address from token_address where token_cd = '{token_cd}'"
        token_address = post_hook.get_first(get_addr_sql)[0]
        utc_str_dt = req_record[1] - timedelta(hours=9)
        utc_end_dt = req_record[2] - timedelta(hours=9)
        col_str_dt_unix = int(time.mktime(utc_str_dt.timetuple()) * 1000)
        col_end_dt_unix = int(time.mktime(utc_end_dt.timetuple()) * 1000)
        task_instance.xcom_push(key="col_req_no", value=col_req_no)
        task_instance.xcom_push(key="token_cd", value=token_cd)
        task_instance.xcom_push(key="token_address", value=token_address)
        task_instance.xcom_push(key="col_str_dt_unix", value=col_str_dt_unix)
        task_instance.xcom_push(key="col_end_dt_unix", value=col_end_dt_unix)
        return "ETL.req_data"
    else :
        return "rm_file"

# api 수집 후 callback
def _http_collback(response, ts_nodash, task_instance) :
    # sql문 만들기
    res_df = pd.DataFrame(response.json()["history"])
    res_df["token_addr"] = task_instance.xcom_pull(key="token_address", task_ids="req_chk")
    values = [f"('{datetime.fromtimestamp(row.date/1000)}', '{row.token_addr}', '{row.rate}')" for row in res_df.itertuples()]
    sql = f""" insert into coin_price_hist (event_timestamp, token_address, price) values {", ".join(values)} on conflict (event_timestamp, token_address) do update set price = excluded.price ;"""
    print(sql)
    # sql문 파일로 저장하기
    file_nm = "price_hist.sql"
    file_path = FILE_FOLDER + "/" + file_nm
    hist_file_path = FILE_FOLDER + "/" + f"{ts_nodash}_" + file_nm
    # 실행용 파일 저장
    with open(file_path, "w") as f:
        f.write(sql)
    # 기록용 파일 저장
    with open(hist_file_path, "w") as f:
        f.write(sql)
    task_instance.xcom_push(key="sql_file_nm", value=file_nm)
    return True



with DAG (
    'collect_token_price_data',
    default_args=default_args,
    description='토큰 가격 수집을 위한 DAG',
    schedule_interval='@once',
    start_date=datetime(2024, 5, 25),
    catchup=False,
    tags=['test']
) as dag:
    req_chk = BranchPythonOperator(
        task_id="req_chk",
        python_callable=_req_chk,
        op_args=["postgres"]
    )

    with TaskGroup("ETL") as etl_grp :
        req_data = SimpleHttpOperator(
            task_id="req_data",
            http_conn_id="livecoinwatch_api",
            endpoint="/coins/single/history",
            headers={
                'content-type': 'application/json',
                'x-api-key': LIVECOINWATCH_API_KEY
            },
            data=json.dumps({
                "currency": "USD",
                "code": "{{ ti.xcom_pull(key='token_cd', task_ids='req_chk') }}",
                "start":  "{{ ti.xcom_pull(key='col_str_dt_unix', task_ids='req_chk') }}",
                "end": "{{ ti.xcom_pull(key='col_end_dt_unix', task_ids='req_chk') }}"
            }),
            response_filter=_http_collback,
            log_response=True
        )

        load_data = PostgresOperator(
            task_id="load_data",
            postgres_conn_id="postgres",
            sql="sql/price_hist.sql",
        )
        req_data >> load_data

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

    rm_file = BashOperator(
        task_id="rm_file",
        bash_command=f"find {FILE_FOLDER} -mtime +7 -delete",
        trigger_rule="all_done"
    )

    req_chk >> etl_grp >> req_update >> rm_file
    req_chk >> rm_file
    etl_grp >> err_handling >> rm_file

