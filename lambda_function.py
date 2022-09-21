import pymysql
from random import randint
from io import StringIO
import sqlalchemy
import boto3
import pandas as pd


## **** CONFIGURATION VARIABLES **** ##
bucket = 'empact-test'

# S3 INIT
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
my_bucket = s3.Bucket(bucket)

## ******* FUNCTIONS ****** ##
# AWS LAMBDA SETUP TARGETS THE 'lambda_handler' FUNCTION IN THE 'lambda_function.py' FILE.
## This is the entry point for the API endpoint being called.
def lambda_handler(event, context):
    print("function called")
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
        # Validate the file/contents

        # Iterate the rows in the dataframe
            # construct the SQL
            # write to RDS Database

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