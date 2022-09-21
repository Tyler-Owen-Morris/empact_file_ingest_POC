import pymysql
import os
from random import randint
from io import StringIO
import sqlalchemy
import pymysql
import boto3
import pandas as pd


## **** CONFIGURATION VARIABLES **** ##
# S3
bucket = 'empact-test'
## tables to be updated
survey_tbl = "detention_survey_tbl"
suspension_tbl = "tbl_suspended_visitations"
# DB ENDPOINT
endpoint = os.environ['DB_DOMAIN']
username = os.environ['DB_USERNAME']
password = os.environ['DB_PASS']
database_name = os.environ['DB_NAME']


# S3 INIT
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
my_bucket = s3.Bucket(bucket)

# SQL INIT
# engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}'.format(username,password,endpoint)).connect()
# sql_tbl = pd.read_sql_table(survey_tbl, engine)
# print("this confirms that the SQL table works properly:",sql_tbl.head())
connection = pymysql.connect(
    host=endpoint, user=username, passwd=password, db=database_name
)
print("conn:",connection)

## ******* FUNCTIONS ****** ##
# AWS LAMBDA SETUP TARGETS THE 'lambda_handler' FUNCTION IN THE 'lambda_function.py' FILE.
## This is the entry point for the API endpoint being called.
def lambda_handler(event, context):
    #print("event:",event)
    #print("content:",context)
    # get the objects in the bucket
    keys = read_from_s3()
    # Iterate through the keys
    for key in keys:
        csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
        print("gotten obj:",csv_obj)
        body = csv_obj['Body'].read().decode('utf-8')
        print("body:",body)
        #csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(body), sep=",")
        print("dataframe:\n",df.head())
        for idx, row in df.iterrows():
            print(row)
        ### Validate the file/contents
        #validate all headers exist
            ## Validate the row values add up to correct values
            ## Validate the data doesn't already exist in the database
        # Write the results to SQL

## UTILITY FUNCTIONS
def read_from_s3():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    ret = []
    for obj in my_bucket.objects.all():
        print("My bucket object: ",obj)
        print("key",obj.key)
        ret.append(obj.key)
    return ret