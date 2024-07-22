from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases if test_cases else []

    def execute(self, context):
        self.log.info('DataQualityOperator is being executed')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for test_case in self.test_cases:
            sql = test_case.get('sql')
            expected_result = test_case.get('expected_result')
            
            records = redshift.get_records(sql)[0]
            if records[0] != expected_result:
                raise ValueError(f"Data quality check failed. SQL: {sql}, Expected: {expected_result}, Got: {records[0]}")
            
            self.log.info(f"Data quality check passed. SQL: {sql}, Expected: {expected_result}")