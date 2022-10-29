from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_stmt = '',
                 truncate_table = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('Setting up connection to Redshift for dimension table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Clearing data from Redshift table: {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        
        self.log.info(f'Loading data into dimension table "{self.table}" on Redshift')
        redshift.run(f'INSERT INTO {self.table} {self.sql_stmt}')
