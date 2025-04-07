from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook  # Correct Import

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
                                      
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table_name='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)  # No need for class name here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        execution_date = context['data_interval_start']  # Extract execution date
        formatted_execution_date = execution_date.strftime('%Y-%m-%d')  # Format properly

        self.log.info(f'Staging {self.table_name} from S3 to Redshift')

        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()

        # Ensure s3_key is formatted before constructing SQL query
        s3_key_with_timestamp = self.s3_key.format(formatted_execution_date)
        s3_path = f's3://{self.s3_bucket}/{s3_key_with_timestamp}'

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Construct SQL query with properly formatted values
        copy_sql = f"""
        COPY {self.table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        JSON '{self.json_path}'
        TIMEFORMAT 'auto';
        """

        self.log.info(f"Running COPY command: {copy_sql}")
        redshift_hook.run(copy_sql)
