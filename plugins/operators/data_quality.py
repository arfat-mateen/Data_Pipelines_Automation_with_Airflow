from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Setting up connection to Redshift for quality check')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.dq_checks:
            self.log.info("No data quality test found!")
            return
        
        self.log.info("Running test to check data quality")
        for check in self.dq_checks:
            test_query, expected_result = check.values()
            
            records = redshift.get_records(test_query)
        
            if records[0][0] != expected_result:
                raise ValueError(f'Data quality check failed. {records[0][0]} does not equal {expected_result}')
            else:
                self.log.info("Data quality check passed")