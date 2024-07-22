from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    template_fields = ('sql_query',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Truncating Redshift table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        else:
            self.log.info(f"Clearing data from destination Redshift table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Inserting data into {self.table}")
        redshift.run(self.sql_query)