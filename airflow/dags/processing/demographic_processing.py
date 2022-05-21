from base64 import encode
from select import select
from pyspark.sql import SparkSession
import boto3
import configparser
from os.path import exists
import json
import pandas as pd
import requests
import numpy as np
from usa_codes import USA_CODE_NAME,USA_NAME_NUMBER


# import boto3
# import os
# from pathlib import Path

# s3 = boto3.resource('s3')

# bucket = s3.Bucket('bucket')

# key = 'product/myproject/2021-02-15/'
# objs = list(bucket.objects.filter(Prefix=key))

# for obj in objs:
#     # print(obj.key)

#     # remove the file name from the object key
#     obj_path = os.path.dirname(obj.key)

#     # create nested directory structure
#     Path(obj_path).mkdir(parents=True, exist_ok=True)

#     # save file with full path locally
#     bucket.download_file(obj.key, obj.key)
    

BUCKET = 'uda-covid-project'
OBJECT_PATH = r'/raw/demographic/us-cities-demographics.json'
TEMP_PATH = r'/home/workspace/airflow/tmp/download/us-cities-demographics.json'
config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dwh.cfg')
AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
S3_PATH = r'/processed/demographic/demographic.csv'

if __name__ == '__main__':
    # spark = SparkSession \
    # .builder \
    # .appName("Python Spark SQL basic example") \
    # .config("spark.some.config.option", "some-value") \
    # .getOrCreate()

    import boto3

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      )

    if not exists(TEMP_PATH):
        s3.download_file(BUCKET, OBJECT_PATH, TEMP_PATH)

    
    #Load the json file
    f = open(TEMP_PATH)
    json_file = json.load(f)
    #Extract all the fields into a list
    json_fields = [dic['fields'] for dic in json_file]
    #Create a dataframe from the list of fields
    df_demographic = pd.DataFrame.from_dict(json_fields)
    #Group By state and race
    df_races = df_demographic[['state','race','count']].groupby(['state','race']).sum()
    #Percentage of race by state
    df_races_percentage = df_races / df_races.groupby('state').transform('sum')
    #Unstacking the result to have 6 columns (1 for state, and 5 for races)
    df_final = df_races_percentage.unstack()
    df_final.columns = df_final.columns.droplevel(level = 1)
    df_final = df_final.rename_axis(None,axis=1)
    df_final = df_final.reset_index()

    #Request to get population by state
    response = requests.get(url='https://api.census.gov/data/2021/pep/population?get=POP_2021,NAME&for=state:*')
    json_population = response.json()
    #Split between headers and data
    json_population_columns = json_population[0]
    json_population_values = json_population[1:]
    #Data to nparray
    np_population = np.array(json_population_values)
    #Create Dataframe from data
    df_population = pd.DataFrame(np_population,columns=json_population_columns)
    #Drop some columns
    df_population.drop('state', axis=1, inplace=True)
    #Renaming columns
    df_population.columns = ['population','state']

    #Merge two dataframes and rename the columns
    df_joined = df_final.merge(df_population, on='state', how='outer')
    df_joined.columns = ['state','american_native','asian','african_american','hispanic','white','population']

    df_joined = df_joined.sort_values(by=['state'])
    df_joined['id_state'] = df_joined['state'].apply(lambda x: USA_NAME_NUMBER[x])
    df_joined.set_index('id_state',inplace=True)

    df_joined.to_csv(r'/home/workspace/airflow/tmp/upload/demographic.csv')
    s3.upload_file(r'/home/workspace/airflow/tmp/upload/demographic.csv', BUCKET,S3_PATH)

