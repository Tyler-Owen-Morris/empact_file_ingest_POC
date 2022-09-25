import pymysql
import os
import datetime
import uuid
from random import randint
from io import StringIO
import sqlalchemy
import pymysql
import boto3
import pandas as pd
from pandas_schema import Column,Schema,validation


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
engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username,password,endpoint,database_name)).connect()
sql_tbl = pd.read_sql_table(survey_tbl, engine)
print("this confirms that the SQL table works properly:",sql_tbl.head())
## DB SCHEMA DEFINITION
schema = Schema([
    Column('SiteID', [validation.InListValidation(sql_tbl['SiteID'].unique())]),
    Column('contactID',[]),
    Column('Survey_Month',[validation.InRangeValidation(0,12)]),
    Column('Survey_Year',[validation.InRangeValidation(2000,2050)]),
    Column('DetPop_First_Day',[]),
    Column('Total_Adm_Prior_Month',[]),
    Column('A1_Race_White',[]),
    Column('A1_Race_Black',[]),
    Column('A1_Race_Hisp',[]),
    Column('A1_Race_Asian',[]),
    Column('A1_Race_Native',[]),
    Column('A1_Race_Pacisl',[]),
    Column('A1_Race_2Plus',[]),
    Column('A1_Race_Other',[]),
    Column('A1_Race_Unknown',[]),
    Column('A1_Race_Refused',[]),
    Column('A2_Race_White',[]),
    Column('A2_Race_Black',[]),
    Column('A2_Race_Hisp',[]),
    Column('A2_Race_Asian',[]),
    Column('A2_Race_Native',[]),
    Column('A2_Race_Pacisl',[]),
    Column('A2_Race_2Plus',[]),
    Column('A2_Race_Other',[]),
    Column('A2_Race_Unknown',[]),
    Column('A2_Race_Refused',[]),
    Column('P1_Race_White',[]),
    Column('P1_Race_Black',[]),
    Column('P1_Race_Hisp',[]),
    Column('P1_Race_Asian',[]),
    Column('P1_Race_Native',[]),
    Column('P1_Race_Pacisl',[]),
    Column('P1_Race_2Plus',[]),
    Column('P1_Race_Other',[]),
    Column('P1_Race_Unknown',[]),
    Column('P1_Race_Refused',[]),
    Column('P2_Race_White',[]),
    Column('P2_Race_Black',[]),
    Column('P2_Race_Hisp',[]),
    Column('P2_Race_Asian',[]),
    Column('P2_Race_Native',[]),
    Column('P2_Race_Pacisl',[]),
    Column('P2_Race_2Plus',[]),
    Column('P2_Race_Other',[]),
    Column('P2_Race_Unknown',[]),
    Column('P2_Race_Refused',[]),
    Column('Admission_Reason_Unable',[]),
    Column('Admission_Reason_New_Offense',[]),
    Column('Admission_Reason_Technical',[]),
    Column('Admission_Reason_Technical',[]),
    Column('Admission_Reason_Post',[]),
    Column('Admission_Reason_Other',[]),
    Column('Admission_Reason_Unknown',[]),
])

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
        valid = schema.validate(df)
        print("len valid:",len(valid))
        print("valid",valid)
        ### Validate the file/contents
        print("dataframe:",df)
        errs = []
        for idx, row in df.iterrows():
            print("row",row)
            print("idx", idx)
            resp = validate_row(row)
            if len(resp) > 0:
                errs.append((idx,resp))
        # Validate the data doesn't already exist in the database
        if len(errs) == 0:
            # Write the results to SQL
            print("modify this DF and write it")
            df['ResponseID'] = "AWS_"+uuid.uuid4().hex
            df['Population_Ethn_Separate_YN'] = ['Yes' if (isinstance(df['P2_Race_White'],int)) else 'No']
            df['Population_Prior_Month_RE_YN'] = ['Yes' if (isinstance(df['P1_Race_White'],int)) else 'No']
            df['Admissions_Prior_Month_RE_YN'] = ['Yes' if (isinstance(df['A1_Race_White'],int)) else 'No']
            df['Admissions_Ethn_Separate_YN'] = ['Yes' if (isinstance(df['A2_Race_White'],int)) else 'No']
            df['Adm_Report_Eth'] = [1 if (isinstance(df['A2_Race_White'],int)) else 2]
            df.to_sql(survey_tbl,engine,if_exists='append',index=False)
        else:
            print("this DF is invalid, send failure text")
            print(errs)
        # remove the processed object
        s3_client.copy_object(bucket,key,"archive/"+key)
        s3.Object(bucket,key).delete()


## UTILITY FUNCTIONS
def read_from_s3():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    ret = []
    for obj in my_bucket.objects.all():
        print("My bucket object: ",obj)
        # print("key",obj.key)
        if obj.key != 'archive/':
            ret.append(obj.key)
    return ret

def validate_row(row):
    resp = []
    #row.fillna(0,inplace=True)
    # Month/year
    mon = int(row['Survey_Month'])
    yr = int(row['Survey_Year'])
    # Population numbers
    DetPop_First_Day = int(row['DetPop_First_Day'])
    print(isinstance(row['P1_Race_White'],int))
    p1w = (row['P1_Race_White'] if isinstance(row['P1_Race_White'],int) else 0) 
    p1b = (row['P1_Race_Black'] if isinstance(row['P1_Race_Black'],int) else 0)
    p1h = (row['P1_Race_Hisp'] if isinstance(row['P1_Race_Hisp'],int) else 0)
    p1a = (row['P1_Race_Asian'] if isinstance(row['P1_Race_Asian'],int) else 0)
    p1n = (row['P1_Race_Native'] if isinstance(row['P1_Race_Native'],int) else 0)
    p1p = (row['P1_Race_Pacisl'] if isinstance(row['P1_Race_Pacisl'],int) else 0 )
    p12 = (row['P1_Race_2Plus'] if isinstance(row['P1_Race_2Plus'],int) else 0)
    p1o = (row['P1_Race_Other'] if isinstance(row['P1_Race_Other'],int) else 0)
    p1u = (row['P1_Race_Unknown'] if isinstance(row['P1_Race_Unknown'],int) else 0)
    p1r = (row['P1_Race_Refused'] if isinstance(row['P1_Race_Refused'],int) else 0)
    p2w = (row['P2_Race_White'] if isinstance(row['P2_Race_White'],int) else 0)
    p2b = (row['P2_Race_Black'] if isinstance(row['P2_Race_White'],int) else 0)
    p2h = (row['P2_Race_Hisp'] if isinstance(row['P2_Race_Hisp'],int) else 0)
    p2a = (row['P2_Race_Asian'] if isinstance(row['P2_Race_Asian'],int) else 0)
    p2n = (row['P2_Race_Native'] if isinstance(row['P2_Race_Native'],int) else 0)
    p2p = (row['P2_Race_Pacisl'] if isinstance(row['P2_Race_Pacisl'],int) else 0)
    p22 = (row['P2_Race_2Plus'] if isinstance(row['P2_Race_2Plus'],int) else 0)
    p2o = (row['P2_Race_Other'] if isinstance(row['P2_Race_Other'],int) else 0)
    p2u = (row['P2_Race_Unknown'] if isinstance(row['P2_Race_Unknown'],int) else 0)
    p2r = (row['P2_Race_Refused'] if isinstance(row['P2_Race_Refused'],int) else 0)
    pop = p1w+p1b+p1h+p1a+p1n+p1p+p12+p1o+p1u+p1r+p2w+p2b+p2h+p2a+p2n+p2p+p22+p2o+p2u+p2r
    print("population",pop)
    # Admissions Numbers
    Total_Adm_Prior_Month = int(row['Total_Adm_Prior_Month'])
    a1w = (row['A1_Race_White'] if isinstance(row['A1_Race_White'],int) else 0)
    a1b = (row['A1_Race_Black'] if isinstance(row['A1_Race_Black'],int) else 0)
    a1h = (row['A1_Race_Hisp'] if isinstance(row['A1_Race_Hisp'],int) else 0)
    a1a = (row['A1_Race_Asian'] if isinstance(row['A1_Race_Asian'],int) else 0)
    a1n = (row['A1_Race_Native'] if isinstance(row['A1_Race_Native'],int) else 0)
    a1p = (row['A1_Race_Pacisl'] if isinstance(row['A1_Race_Pacisl'],int) else 0)
    a12 = (row['A1_Race_2Plus'] if isinstance(row['A1_Race_2Plus'],int) else 0)
    a1o = (row['A1_Race_Other'] if isinstance(row['A1_Race_Other'],int) else 0)
    a1u = (row['A1_Race_Unknown'] if isinstance(row['A1_Race_Unknown'],int) else 0)
    a1r = (row['A1_Race_Refused'] if isinstance(row['A1_Race_Refused'],int) else 0)
    a2w = (row['A2_Race_White'] if isinstance(row['A2_Race_White'],int) else 0)
    a2b = (row['A2_Race_Black'] if isinstance(row['A2_Race_Black'],int) else 0)
    a2h = (row['A2_Race_Hisp'] if isinstance(row['A2_Race_Hisp'],int) else 0)
    a2a = (row['A2_Race_Asian'] if isinstance(row['A2_Race_Asian'],int) else 0)
    a2n = (row['A2_Race_Native'] if isinstance(row['A2_Race_Native'],int) else 0)
    a2p = (row['A2_Race_Pacisl'] if isinstance(row['A2_Race_Pacisl'],int) else 0)
    a22 = (row['A2_Race_2Plus'] if isinstance(row['A2_Race_2Plus'],int) else 0)
    a2o = (row['A2_Race_Other'] if isinstance(row['A2_Race_Other'],int) else 0)
    a2u = (row['A2_Race_Unknown'] if isinstance(row['A2_Race_Unknown'],int) else 0)
    a2r = (row['A2_Race_Refused'] if isinstance(row['A2_Race_Refused'],int) else 0)
    adm = a1w+a1b+a1h+a1a+a1n+a1p+a12+a1o+a1u+a1r+a2w+a2b+a2h+a2a+a2n+a2p+a22+a2o+a2u+a2r
    # Validation
    if mon < 1 or mon > 12:
        resp.append("Survey_Month Invalid")
    if yr < 2000 or yr >2050:
        resp.append("Survey_Year Invalid")
    if pop != DetPop_First_Day:
        resp.append("Population counts to not match reported totals")
    if adm != Total_Adm_Prior_Month:
        resp.append("Admission counts do not match reported totals")
    exi = sql_tbl.query("Survey_Year == "+str(row['Survey_Year'])+" and Survey_Month == "+str(row['Survey_Month'])+" and SiteID == '"+row['SiteID']+"'")
    print(">>>>> exists:",exi.shape)
    if exi.shape[0] > 0:
        resp.append("Row data already exists")
    return resp


    