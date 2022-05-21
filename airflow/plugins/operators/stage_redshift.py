from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import configparser

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 s3_bucket = '',
                 s3_key = '',
                 json_path = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        
        config = configparser.ConfigParser()
        config.read('/home/workspace/airflow/dwh.cfg')
        AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
        AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        fill_auto = "'auto'"
        if self.json_path == 'CSV':
            fill_auto = ''
        
        statement = f"""
                    COPY {self.table} 
                    FROM 's3://{self.s3_bucket}/{self.s3_key}' 
                    ACCESS_KEY_ID '{AWS_ACCESS_KEY_ID}'
                    SECRET_ACCESS_KEY '{AWS_SECRET_ACCESS_KEY}'
                    REGION 'us-west-2' 
                    FORMAT AS {self.json_path}
                    {fill_auto}
                    IGNOREHEADER 1
                    TIMEFORMAT as 'epochmillisecs'
                    DELIMITER ','
                    FILLRECORD
                    TRUNCATECOLUMNS blanksasnull emptyasnull
                    """
        
        redshift.run(statement)
