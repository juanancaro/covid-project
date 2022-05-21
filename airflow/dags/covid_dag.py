from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadTableOperator,
                               DataQualityOperator)
from airflow.operators.bash_operator import BashOperator
from helpers import SqlQueries


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depend_on_past': False
}

dag = DAG('covid_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

process_demographic = BashOperator(
    task_id='transform_demographic',
    bash_command = '/opt/conda/bin/python3 /home/workspace/airflow/dags/processing/demographic_processing.py',
    dag=dag
)

process_education = BashOperator(
    task_id='transform_education',
    bash_command = '/opt/conda/bin/python3 /home/workspace/airflow/dags/processing/education_processing.py',
    dag=dag
)

process_temperature = BashOperator(
    task_id='transform_temperature',
    bash_command = '/opt/conda/bin/python3 /home/workspace/airflow/dags/processing/temperature_processing.py',
    dag=dag
)

process_covid = BashOperator(
    task_id='transform_covid',
    bash_command = '/opt/conda/bin/python3 /home/workspace/airflow/dags/processing/covid_processing.py',
    dag=dag
)

tables_to_redshift = PostgresOperator(
    task_id = 'create_table_events',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographic',
    dag=dag,
    table='staging_demographic',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='uda-covid-project',
    s3_key='/processed/demographic/demographic.csv',
    json_path = 'CSV'
)

stage_education_to_redshift = StageToRedshiftOperator(
    task_id='Stage_education',
    dag=dag,
    table='staging_education',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='uda-covid-project',
    s3_key='/processed/education/education.csv',
    json_path = 'CSV'
)

stage_temperatures_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperatures',
    dag=dag,
    table='staging_temperature',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='uda-covid-project',
    s3_key='/processed/temperature/temperature.csv',
    json_path = 'CSV'
)

stage_covid_to_redshift = StageToRedshiftOperator(
    task_id='Stage_covid',
    dag=dag,
    table='staging_covid',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='uda-covid-project',
    s3_key='/processed/covid/covid.csv',
    json_path = 'CSV'
)

load_education_table = LoadTableOperator(
    task_id='Load_education_instituion_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='education',
    sql=SqlQueries.education_table_insert
)

load_state_table = LoadTableOperator(
    task_id='Load_state_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='state',
    sql=SqlQueries.state_table_insert
)

load_covid_table = LoadTableOperator(
    task_id='Load_covid_instituion_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='covid',
    sql=SqlQueries.covid_table_insert
)

run_quality_checks_1 = DataQualityOperator(
    task_id='Run_data_quality_checks_1',
    dag=dag,
    redshift_conn_id='redshift',
    dq_tables = ['education','state','covid'],
    id_tables = ['id_institution','id_state','id_covid_data'],
    mode = '1'
)

run_quality_checks_2 = DataQualityOperator(
    task_id='Run_data_quality_checks_2',
    dag=dag,
    redshift_conn_id='redshift',
    dq_tables = ['education','state','covid'],
    id_tables = [],
    mode = '2'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> process_demographic
start_operator >> process_education
start_operator >> process_temperature
start_operator >> process_covid

process_demographic >> tables_to_redshift
process_education >> tables_to_redshift
process_temperature >> tables_to_redshift
process_covid >> tables_to_redshift

tables_to_redshift >> stage_demographic_to_redshift
tables_to_redshift >> stage_education_to_redshift
tables_to_redshift >> stage_temperatures_to_redshift
tables_to_redshift >> stage_covid_to_redshift


stage_demographic_to_redshift >> load_state_table
stage_education_to_redshift >> load_education_table
stage_temperatures_to_redshift >> load_state_table
stage_covid_to_redshift >> load_covid_table


load_state_table >> run_quality_checks_1 
load_education_table >> run_quality_checks_1 
load_covid_table >> run_quality_checks_1
load_state_table >> run_quality_checks_2
load_education_table >> run_quality_checks_2 
load_covid_table >> run_quality_checks_2

run_quality_checks_1 >> end_operator
run_quality_checks_2 >> end_operator
