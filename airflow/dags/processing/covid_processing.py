from base64 import encode
from select import select
from pyspark.sql import SparkSession
import boto3
import configparser
from os.path import exists
import pandas as pd
import numpy as np
from usa_codes import USA_CODE_NAME,USA_NAME_NUMBER

BUCKET = r'uda-covid-project'
OBJECT_PATH = r'/raw/covid/'
OBJECT_COVID = r'us_states_covid19_daily.csv'
TEMP_PATH = r'/home/workspace/airflow/tmp/download/'

config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dwh.cfg')
AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
S3_PATH = r'/processed/covid/covid.csv'



if __name__ == '__main__':

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      )

    if not exists(TEMP_PATH + OBJECT_COVID):
        s3.download_file(BUCKET, OBJECT_PATH + OBJECT_COVID, TEMP_PATH + OBJECT_COVID)
        

    df_covid = pd.read_csv(TEMP_PATH + OBJECT_COVID)
    #Dropping all unnecessary columns
    df_covid = df_covid[['date','state','positive','hospitalizedCurrently','hospitalizedCumulative','death']]
    #Date column to datetime format
    df_covid['date'] = pd.to_datetime(df_covid['date'],format='%Y%m%d')
    #Changing state to name
    df_covid['state'] = df_covid['state'].apply(lambda x: USA_CODE_NAME[x])
    #Removing american samoa because 0 positive
    df_covid = df_covid[df_covid['state'] != 'American Samoa']
    #Filling al NaN values with 0
    df_covid.fillna(0, inplace=True)

    df_covid = df_covid.sort_values(by=['date'])
    df_covid.reset_index(inplace=True)
    df_covid.drop(['index'],axis=1,inplace=True)
    df_covid.index.name = 'id_covid_data'

    new_columns = ['date','state','positive','hospitalized_curr','hospizalited_acum','death']
    df_covid.columns = new_columns
    df_covid['id_state'] = df_covid['state'].apply(lambda x: USA_NAME_NUMBER[x])


    df_covid.to_csv(r'/home/workspace/airflow/tmp/upload/covid.csv')
    s3.upload_file(r'/home/workspace/airflow/tmp/upload/covid.csv', BUCKET,S3_PATH)






    