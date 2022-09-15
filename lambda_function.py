import pymysql
import json
from random import randint
import os
import csv
from io import StringIO, BytesIO, FileIO
import requests
import boto3
import pandas as pd


## **** CONFIGURATION VARIABLES **** ##
bucket = 'walkersftptest'

# S3 INIT
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
my_bucket = s3.Bucket(bucket)

## ******* FUNCTIONS ****** ##
# AWS LAMBDA SETUP TARGETS THE 'lambda_handler' FUNCTION IN THE 'lambda_function.py' FILE.
## This is the entry point for the API endpoint being called.
def lambda_handler(event, context):
    print("function called")
    read_from_s3()

## UTILITY FUNCTIONS
def read_from_s3():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    ret = []
    for obj in my_bucket.objects.all():
        print(obj)
        ret.append(obj)
    return ret