import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')
AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
bucket = 'uda-covid-project'
s3 = boto3.client('s3',region_name="us-west-2",aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

#Demographics
s3.upload_file(r'data\demographic\us-cities-demographics.json', bucket, '/raw/demographic/us-cities-demographics.json')
print("Demographic data uploaded!")
#Education
s3.upload_file(r'data\schools\Private_Schools.csv', bucket, '/raw/education/private_schools.csv')
s3.upload_file(r'data\schools\Public_Schools.csv', bucket, '/raw/education/public_schools.csv')
s3.upload_file(r'data\university\Colleges_and_Universities.csv', bucket, '/raw/education/colleges_universities.csv')
print("Education data uploaded!")
#Temperatures
s3.upload_file(r'data\temperatures\GlobalLandTemperaturesByState.csv', bucket, '/raw/temperatures/temperatures_by_state.csv')
print("Temperature data uploaded!")
#Covid
s3.upload_file(r'data\covid\us_states_covid19_daily.csv', bucket, '/raw/covid/us_states_covid19_daily.csv')
s3.upload_file(r'data\covid\us_counties_covid19_daily.csv', bucket,'/raw/covid/us_counties_covid19_daily.csv')
print("Covid data uploaded!")

