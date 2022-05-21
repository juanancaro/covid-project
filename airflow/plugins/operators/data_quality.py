from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 dq_tables = '',
                 id_tables = '',
                 mode = 1,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.dq_tables = dq_tables
        self.id_tables = id_tables
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == '1':
            for table,id_t in zip(self.dq_tables,self.id_tables):
                result = redshift.get_records(f'SELECT COUNT(*) FROM {table} WHERE {id_t} IS NULL')

                if result[0][0] > 0:
                    raise ValueError('There are nulls in {table}')
        else:
            result = redshift.get_records(f'SELECT COUNT(*) FROM education WHERE n_students < 0.0')
            if result[0][0] > 0:
                raise ValueError('There are values below 0 in education')
         
                
        self.log.info('Data Quality done succesfully')