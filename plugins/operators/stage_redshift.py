from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)
    copy_sql_stmt = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS json '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = '',
                 redshift_conn_id = '',
                 s3_bucket = '',
                 s3_key = '',
                 region = '',
                 table = '',
                 json_format = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.table = table
        self.json_format = json_format

    def execute(self, context):
        self.log.info('Setting up connection to Redshift for staging data')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Clearing data from Redshift table: {self.table}')
        redshift.run(f'TRUNCATE {self.table}')
        
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'
        
        formatted_sql_stmt = StageToRedshiftOperator.copy_sql_stmt.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
        
        self.log.info(f'Copying data from "{s3_path}" to Redshift table "{self.table}"')
        redshift.run(formatted_sql_stmt)
