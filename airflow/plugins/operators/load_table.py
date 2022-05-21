from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql = '',        
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        statement = f'INSERT INTO {self.table} {self.sql}'
        redshift.run(statement)
