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
OBJECT_PATH = r'/raw/temperatures/'
OBJECT_TEMPERATURE = r'temperatures_by_state.csv'
TEMP_PATH = r'/home/workspace/airflow/tmp/download/'

config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dwh.cfg')
AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
S3_PATH = r'/processed/temperature/temperature.csv'

if __name__ == '__main__':

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      )

    if not exists(TEMP_PATH + OBJECT_TEMPERATURE):
        s3.download_file(BUCKET, OBJECT_PATH + OBJECT_TEMPERATURE, TEMP_PATH + OBJECT_TEMPERATURE)
        

    df_temperature = pd.read_csv(TEMP_PATH + OBJECT_TEMPERATURE)
    #Filter all cities not from United States
    df_temperature = df_temperature[df_temperature['Country'] == 'United States']
    #dt column to date as datetime
    df_temperature['date'] = pd.to_datetime(df_temperature['dt'],format='%Y-%m-%d')
    #Filter date before 1970
    df_temperature = df_temperature[df_temperature['date'] > '1969-12-31']
    #Drop unnecessary columns and some NaN values
    df_temperature.drop(['dt','AverageTemperatureUncertainty','Country'],inplace=True, axis=1)
    df_temperature.dropna()
    #Season temperatures
    df_temperature.loc[df_temperature['date'].dt.month <= 3, 'season'] = 'winter'
    df_temperature.loc[(df_temperature['date'].dt.month >= 4) & (df_temperature['date'].dt.month <= 6) , 'season'] = 'spring'
    df_temperature.loc[(df_temperature['date'].dt.month >= 7) & (df_temperature['date'].dt.month <= 9) , 'season'] = 'summer'
    df_temperature.loc[df_temperature['date'].dt.month >= 10, 'season'] = 'autumn'        
    #Changing Georgia (State) to Georgia
    df_temperature.loc[df_temperature['State'] == 'Georgia (State)', 'State'] = 'Georgia'
    #Renaming columns
    new_columns = ['avg_temp','state','date','season']
    df_temperature.columns = new_columns
    #Compute total avg by state
    df_total_avg = df_temperature[['avg_temp','state']].groupby('state').mean()
    df_total_avg = df_total_avg.reset_index()
    #Compute total avg by state and by season
    df_season_avg = df_temperature[['avg_temp','state','season']].groupby(['state','season']).mean()
    df_season_avg = df_season_avg.unstack()
    df_season_avg.columns = df_season_avg.columns.droplevel(level = 1)
    df_season_avg.columns = ['autumn','spring','summer','winter']
    df_season_avg = df_season_avg.rename_axis(None,axis=1)
    df_season_avg = df_season_avg.reset_index()
    #Join the total and the season avg
    df_temperatures = df_total_avg.merge(df_season_avg, on='state', how='outer')
    
    df_temperatures.sort_values(by=['state'])
    df_temperatures.index.name = 'id_state'
    df_temperatures['id_state'] = df_temperatures['state'].apply(lambda x: USA_NAME_NUMBER[x])
    df_temperatures.set_index('id_state',inplace=True)

    

    df_temperatures.to_csv(r'/home/workspace/airflow/tmp/upload/temperatures.csv')
    s3.upload_file(r'/home/workspace/airflow/tmp/upload/temperatures.csv', BUCKET,S3_PATH)






    