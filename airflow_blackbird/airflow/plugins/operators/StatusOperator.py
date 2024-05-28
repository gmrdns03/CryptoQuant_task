from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class StatusOperator(BaseOperator) :
    @apply_defaults
    def __init__(self, conn_id, status, xcom_key, xcom_task_ids, **kwargs) :
        super().__init__(**kwargs)
        self.post_hook = PostgresHook(postgres_conn_id=conn_id)
        self.xcom_key = xcom_key
        self.xcom_task_ids = xcom_task_ids
        self.status = status

    def execute(self, context, *args, **kwargs) :
        trg_req_no = context.get("ti").xcom_pull(key=self.xcom_key, task_ids=self.xcom_task_ids)
        sql = f"update collect_req set col_stat = '{self.status}' where col_req_no = {trg_req_no}"
        self.post_hook.run(sql)

