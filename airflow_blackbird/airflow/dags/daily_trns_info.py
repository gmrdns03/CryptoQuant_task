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
        where col_stat = 'N' and col_id = 'trns' order by col_req_no limit 1
    """
    req_record = post_hook.get_first(sql)
    if req_record is not None and len(req_record) > 0 :
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

def _trns_data_et(conn_id, token_addr, col_str_dt, col_end_dt, ts_nodash, task_instance) :
    from airflow.providers.postgres.hooks.postgres import  PostgresHook
    post_hook = PostgresHook(postgres_conn_id=conn_id)

    str_dt_utc = datetime.strptime(col_str_dt, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    end_dt_utc = datetime.strptime(col_end_dt, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    str_dt = datetime.strftime(str_dt_utc, "%Y-%m-%d")
    end_dt = datetime.strftime(end_dt_utc, "%Y-%m-%d")
    sql=f"""
        select
            trans.dt,
            trans.token_addr as token_address,
            trans.from_addr_cnt as from_addr_cnt,
            trans.to_addr_cnt as to_addr_cnt,
            trans.qntt_trns_amt as qntt_trns_amt,
            COALESCE(price.avg_price,price.avg_price,0) as avg_price,
            case
                when price.avg_price is not null then (trans.qntt_trns_amt * price.avg_price)
                else null
            end as trns_amt
        from (
            select
                A.token_address as token_addr,
                TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') as dt,
                count(distinct A.from_address) as from_addr_cnt,
                count(distinct A.to_address) as to_addr_cnt,
                sum(A.amount) as qntt_trns_amt
            from onchain_trans_log A
            where
                A.token_address = '{token_addr}'
                and TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') between '{str_dt}' and '{end_dt}'
            group by A.token_address, TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') ) trans
            left join (select
                            TO_CHAR(B.event_timestamp, 'YYYY-MM-DD') as dt,
                            avg(B.price) as avg_price
                        from coin_price_hist B
                        where
                            B.token_address = '{token_addr}'
                            and TO_CHAR(B.event_timestamp, 'YYYY-MM-DD') between '{str_dt}' and '{end_dt}'
                        group by TO_CHAR(B.event_timestamp, 'YYYY-MM-DD') ) price
            on trans.dt = price.dt
    """
    src_df = post_hook.get_pandas_df(sql)
    src_df_daily_set = set(src_df["dt"])
    dt_ls = gen_daily_dt(str_dt, end_dt)
    dt_set = set(dt_ls)
    need_days = list(dt_set - src_df_daily_set)
    # 추가할 행 만들기
    res_df = pd.DataFrame(columns=["dt"], data=need_days)
    res_df["token_address"] = token_addr
    res_df["from_addr_cnt"] = 0
    res_df["to_addr_cnt"] = 0
    res_df["qntt_trns_amt"] = 0
    res_df["avg_price"] = 0
    res_df["trns_amt"] = None
    # 데이터 프레임 합치기
    res_df = pd.concat([src_df, res_df], axis=0)
    # sql문 만들기
    values = [ f"('{row.dt}', '{row.token_address}', '{row.from_addr_cnt}', '{row.to_addr_cnt}', '{row.qntt_trns_amt}', '{row.avg_price}', '{row.trns_amt}')" for row in res_df.itertuples()]
    upsert_sql = f""" insert into daily_trns_info (dt, token_address, from_addr_cnt, to_addr_cnt, qntt_trns_amt, avg_price, trns_amt) values {", ".join(values)} on conflict (dt, token_address) do update set (from_addr_cnt, to_addr_cnt, qntt_trns_amt, avg_price, trns_amt) = (excluded.from_addr_cnt, excluded.to_addr_cnt, excluded.qntt_trns_amt, excluded.avg_price, excluded.trns_amt) ; """
    # 파일로 저장하기
    file_nm = "daliy_trns_info.sql"
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
    'daily_transaction_amount_checker',
    default_args=default_args,
    description='일별 거래량 데이터와 거래 참여자수 DAG',
    schedule_interval='@once',
    start_date=datetime(2024, 5, 25),
    catchup=False,
    tags=['']
) as dag :
    req_chk = BranchPythonOperator(
        task_id="req_chk",
        python_callable=_req_chk,
        op_args=["postgres"]
    )

    req_data = PythonOperator(
        task_id="req_data",
        python_callable=_trns_data_et,
        op_kwargs={
            "conn_id" : "postgres",
            "token_addr" : "0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74",
            "col_str_dt" : "{{ ti.xcom_pull(key='col_str_dt', task_ids='req_chk') }}",
            "col_end_dt" : "{{ ti.xcom_pull(key='col_end_dt', task_ids='req_chk') }}"
        }
    )

    load_data = PostgresOperator(
        task_id="load_data",
        postgres_conn_id="postgres",
        sql="sql/daliy_trns_info.sql",
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
