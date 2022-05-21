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
OBJECT_PATH = r'/raw/education/'
OBJECT_PUBLIC = r'public_schools.csv'
OBJECT_PRIVATE = r'private_schools.csv'
OBJECT_COLLEGUE = r'colleges_universities.csv'
TEMP_PATH = r'/home/workspace/airflow/tmp/download/'

config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dwh.cfg')
AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
S3_PATH = r'/processed/education/education.csv'



if __name__ == '__main__':

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      )

    if not exists(TEMP_PATH + OBJECT_PUBLIC):
        s3.download_file(BUCKET, OBJECT_PATH + OBJECT_PUBLIC, TEMP_PATH + OBJECT_PUBLIC)
        

    df_public = pd.read_csv(TEMP_PATH + OBJECT_PUBLIC)

    if not exists(TEMP_PATH + OBJECT_PRIVATE):
        s3.download_file(BUCKET, OBJECT_PATH + OBJECT_PRIVATE, TEMP_PATH + OBJECT_PRIVATE)
        

    df_private = pd.read_csv(TEMP_PATH + OBJECT_PRIVATE)

    #New column for availabity {private,public}
    df_public['AVAILABILTY'] = 'public'
    df_private['AVAILABILTY'] = 'private'
    #Dropping FID and renaming START_GRAD to concat the two dataframes
    df_private.drop(['FID'], axis=1,inplace=True)
    df_private.rename(columns={'START_GRAD':'ST_GRADE'}, inplace=True)
    df_schools = pd.concat([df_public,df_private])
    #Reset the index of the concatenated dataframe
    df_schools.reset_index(inplace=True)
    #Dropping unnecessary columns
    df_schools.drop(['X','Y','OBJECTID','NCESID','ADDRESS','ZIP','ZIP4','TELEPHONE','TYPE', 'STATUS', 'POPULATION','COUNTYFIPS','LATITUDE','LONGITUDE',
    'NAICS_CODE', 'NAICS_DESC', 'SOURCE', 'SOURCEDATE', 'VAL_METHOD', 'VAL_DATE', 'WEBSITE','LEVEL_', 'ST_GRADE', 'END_GRADE', 'DISTRICTID',
    'SHELTER_ID'],inplace=True, axis=1)
    #Fixing enrollment and ft_teacher columns
    value = df_schools['ENROLLMENT'][df_schools['ENROLLMENT'] > 0].mean()
    df_schools['ENROLLMENT'][df_schools['ENROLLMENT'] < 0] = value
    value = df_schools['FT_TEACHER'][df_schools['FT_TEACHER'] > 0].mean()
    df_schools['FT_TEACHER'][df_schools['FT_TEACHER'] < 0] = value
    #Renaming columns
    new_columns = ['id_institution','availability','city','county','country','n_students','n_employees','name','state']
    df_schools.columns = new_columns
    df_schools = df_schools.reindex(columns=['id_institution','name','city','state','county','country','n_students','n_employees','availability'])
    #Dropping null values
    df_schools.dropna(inplace=True)
    

    if not exists(TEMP_PATH + OBJECT_COLLEGUE):
        s3.download_file(BUCKET, OBJECT_PATH + OBJECT_COLLEGUE, TEMP_PATH + OBJECT_COLLEGUE)
        

    df_collegue = pd.read_csv(TEMP_PATH + OBJECT_COLLEGUE)
    #Drop unnecessary columns
    df_collegue.drop(['index', 'X', 'Y', 'FID', 'IPEDSID', 'ADDRESS', 'ADDRESS2',
       'ZIP', 'ZIP4', 'TELEPHONE', 'TYPE', 'STATUS',
       'POPULATION', 'COUNTYFIPS', 'LATITUDE',
       'LONGITUDE', 'NAICS_CODE', 'NAICS_DESC', 'SOURCE', 'SOURCE_DAT',
       'VAL_METHOD', 'VAL_DATE', 'WEBSITE', 'STFIPS', 'COFIPS', 'SECTOR',
       'LEVEL_', 'HI_OFFER', 'DEG_GRANT', 'LOCALE', 'CLOSE_DATE', 'MERGE_ID',
       'ALIAS', 'SIZE_SET', 'INST_SIZE', 'PT_ENROLL', 'FT_ENROLL',
       'HOUSING', 'DORM_CAP', 'SHELTER_ID'],inplace=True, axis=1)
    
    df_collegue.reset_index(inplace=True)
    #Creating availability column to match the other dataframe
    df_collegue['availability'] = 'No data'
    #Renaming coluns
    
    df_collegue.columns = ['id_institution','name','city','state','county','country','n_students','n_employees','availability']

    df_education = pd.concat([df_schools,df_collegue],ignore_index=True)
    
    #Removing educational institution with 0 employees
    #df_education = df_education[df_education['n_employees'] > 0]
    #Changing state to name
    df_education['state'] = df_education['state'].apply(lambda x: USA_CODE_NAME.get(x,'Alaska'))
    #Dropping unnecessary columns
    df_education.drop(['id_institution','county','country'],inplace=True, axis=1)
    
    
    new_columns = ['name','city','state','n_students','n_employees','availability']
    df_education.columns = new_columns
    df_education.index.name = 'id_institution'
    df_education['id_state'] = df_education['state'].apply(lambda x: USA_NAME_NUMBER.get(x,0))

    
    print(df_education.head(5))
    
    df_education.to_csv(r'/home/workspace/airflow/tmp/upload/education.csv')
    s3.upload_file(r'/home/workspace/airflow/tmp/upload/education.csv', BUCKET,S3_PATH)






    