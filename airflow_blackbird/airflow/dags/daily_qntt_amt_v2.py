from airflow import DAG
from datetime import datetime, timedelta

# airflow
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable

# custom operators
from operators.StatusOperator import StatusOperator

# libs
import pandas as pd

LIVECOINWATCH_API_KEY = Variable.get("livecoinwatch-api-key")
SQL_FILE_FOLDER = Variable.get("sql_folder_path")

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# 날짜 생성 함수
def gen_daily_dt(str_dt:str, end_dt:str) -> list :
    curr_dt = datetime.strptime(str_dt, "%Y-%m-%d")
    end_dt = datetime.strptime(end_dt, "%Y-%m-%d")
    dt_ls = []
    while curr_dt <= end_dt :
        dt_ls.append(curr_dt.strftime("%Y-%m-%d"))
        curr_dt += timedelta(days=1)
    return dt_ls


# 수집 요청 테이블 확인
def _req_chk(conn_id, task_instance) :
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    post_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        select col_req_no, col_str_dt, col_end_dt, token_cd from collect_req
        where col_stat = 'N' and col_id = 'qntt' order by col_req_no limit 1
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
        return "req_data"
    else :
        return "rm_file"

def _qntt_data_et(conn_id, user_addr, token_addr, col_str_dt, col_end_dt, ts_nodash, task_instance) :
    from airflow.providers.postgres.hooks.postgres import  PostgresHook
    post_hook = PostgresHook(postgres_conn_id=conn_id)

    str_dt_utc = datetime.strptime(col_str_dt, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    end_dt_utc = datetime.strptime(col_end_dt, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    str_dt = datetime.strftime(str_dt_utc, "%Y-%m-%d")
    end_dt = datetime.strftime(end_dt_utc, "%Y-%m-%d")
    sql = f"""
        select
            D.dt,
            coalesce(Q.addr, '{user_addr}') as user_addr,
            coalesce(Q.token_address, '{token_addr}') as token_address,
            coalesce(Q.qntt_amt, 0) as qntt_amt
        from
            (select TO_CHAR(generate_series(
                '{str_dt}'::date, '{end_dt}'::date, '1 day'::interval
            ), 'YYYY-MM-DD') as dt) D
            left join (
                select
                    COALESCE(t.dt, t.dt, f.dt) as dt,
                    COALESCE(t.addr, t.addr, f.addr) as addr,
                    t.token_address as token_address,
                    COALESCE(t.acct, t.acct, 0) - COALESCE(f.sent, f.sent, 0) as qntt_amt
                from
                    (select
                        TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') as dt,
                        A.to_address as addr,
                        A.token_address as token_address,
                        SUM(A.amount) as acct
                    from onchain_trans_log A
                    where
                        A.to_address = '{user_addr}'
                        and A.token_address = '{token_addr}'
                        and TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') between '{str_dt}' and '{end_dt}'
                    group by  A.token_address, A.to_address, TO_CHAR(A.block_timestamp, 'YYYY-MM-DD')) t
                    full outer join
                    (select
                        TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') as dt,
                        A.from_address as addr,
                        A.token_address as token_address,
                        SUM(A.amount) as sent
                    from onchain_trans_log A
                    where
                        A.from_address = '{user_addr}'
                        and A.token_address = '{token_addr}'
                        and TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') between '{str_dt}' and '{end_dt}'
                    group by  A.token_address, A.to_address, A.from_address, TO_CHAR(A.block_timestamp, 'YYYY-MM-DD')) f
                    on t.dt = f.dt
            ) Q on D.dt = Q.dt
    """
    src_df = post_hook.get_pandas_df(sql)
    print(src_df)
    # sql문 만들기
    values = [f"('{row.dt}', '{row.user_addr}', '{row.token_address}', '{row.qntt_amt}')" for row in src_df.itertuples()]
    upsert_sql = f""" insert into daliy_user_qntt_amt (dt, user_addr, token_address, qntt_amt) values {", ".join(values)} on conflict (dt, user_addr, token_address) do update set qntt_amt = excluded.qntt_amt ;"""
    # 파일로 저장하기
    file_nm = "daliy_user_qntt_amt.sql"
    file_path = SQL_FILE_FOLDER + '/' + file_nm
    hist_file_path = SQL_FILE_FOLDER + '/' + f"{ts_nodash}_{file_nm}"
    # 실행용 파일 저장
    with open(file_path, "w") as f:
        f.write(upsert_sql)
    # 기록용 파일 저장
    with open(hist_file_path, "w") as f:
        f.write(upsert_sql)
    task_instance.xcom_push(key="sql_file_nm", value=file_nm)
    return True


with DAG (
    'daily_Quantity_amount_checker_v2',
    default_args=default_args,
    description='계정의 일별 보유량 확인 DAG',
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

    req_data = PythonOperator(
        task_id="req_data",
        python_callable=_qntt_data_et,
        op_kwargs={
            "conn_id" : "postgres",
            "user_addr" : "0x28c6c06298d514db089934071355e5743bf21d60",
            "token_addr" : "0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74",
            "col_str_dt" : "{{ ti.xcom_pull(key='col_str_dt', task_ids='req_chk') }}",
            "col_end_dt" : "{{ ti.xcom_pull(key='col_end_dt', task_ids='req_chk') }}"
        }
    )

    load_data = PostgresOperator(
        task_id="load_data",
        postgres_conn_id="postgres",
        sql="sql/daliy_user_qntt_amt.sql",
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

    rm_file = BashOperator(
        task_id="rm_file",
        bash_command=f"find {SQL_FILE_FOLDER} -mtime +7 -delete",
        trigger_rule="all_done"
    )

    req_chk >> req_data >> load_data >> req_update >> rm_file
    [req_data, load_data] >> err_handling >> rm_file
    req_chk >> rm_file
