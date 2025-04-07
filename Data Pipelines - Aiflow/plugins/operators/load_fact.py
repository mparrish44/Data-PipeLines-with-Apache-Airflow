from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 select_sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info(f'Loading data into {self.table_name}')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_sql = f"""
            INSERT INTO {self.table_name}
            {self.select_sql}
            """

        self.log.info(f"Running INSERT command: {insert_sql}")
        redshift_hook.run(insert_sql)
