from urllib import response
from numpy import isnan
import pymysql
import os
from datetime import datetime
import uuid
from random import randint
from io import StringIO, BytesIO
import sqlalchemy
import boto3
import pandas as pd
from pandas_schema import Column,Schema,validation
from botocore.exceptions import ClientError


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
ses_client = boto3.client('ses')
my_bucket = s3.Bucket(bucket)

# SQL INIT
engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username,password,endpoint,database_name)).connect()
sql_tbl = pd.read_sql_table(survey_tbl, engine)
print("this confirms that the SQL table works properly:",sql_tbl.head())
## DB SCHEMA DEFINITION
schema = Schema([
    Column('SiteID', [validation.InListValidation(sql_tbl['SiteID'].unique())]),
    Column('contactID',[]),
    Column('Survey_Month',[validation.InRangeValidation(1,13)]),
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
file_err = []
def lambda_handler(event, context):
    # print("event:",event)
    # print("content:",context)
    # get the objects in the bucket
    keys = read_from_s3()
    # Iterate through the keys
    for key in keys:
        mykey = key
        csv_obj = s3_client.get_object(Bucket=bucket, Key=mykey)
        #print("gotten obj:",csv_obj)
        body = csv_obj['Body'].read().decode('utf-8')
        #print("body:",body)
        #csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(body), sep=",")
        ### Validate the file/contents
        #print("dataframe:",df)
        valid = schema.validate(df)
        # print("len valid:",len(valid))
        # print("valid",valid)
        errs = []
        succs = []
        if len(valid) > 0:
            ds =[]
            for err in valid:
                # This sanitizes the full SiteID list from being returned in the email
                clean_err = str(err)
                if "options" in str(err):
                    clean_err = str(err).split("options")[0]+"options"
                ds.append(clean_err)
            # This apppends the structure errors to the main list of errors
            errs.append((err.row+1,ds))
        try:
            for idx, row in df.iterrows():
                #print("row",row)
                #print("idx", idx)
                resp = validate_row(row)
                if len(resp) > 0:
                    errs.append((idx,resp))
                else:
                    succs.append((row['SiteID'], str(row['Survey_Month']) +"/"+str(row['Survey_Year'])))
        except:
            print("this is passing")
            pass
        # Validate the data doesn't already exist in the database
        if len(errs) == 0:
            # Write the results to SQL
            print("modify this DF and write it")
            df['ResponseID'] = "AWS_"+uuid.uuid4().hex
            df['Population_Prior_Month_RE_YN'] = df.apply(pop_prior_month_cond,axis=1) 
            df['Population_Ethn_Separate_YN'] = df.apply(pop_eth_sep_cond,axis=1)
            df['Admissions_Prior_Month_RE_YN'] = df.apply(adm_prior_month_cond,axis=1)
            df['Admissions_Ethn_Separate_YN'] = df.apply(adm_eth_sep_cond,axis=1)
            df['Adm_Report_Eth'] = df.apply(adm_eth_sep_cond,axis=1)
            df['Recorded_Date'] = df.apply(get_formatted_datetime,axis=1)
            df.to_sql(survey_tbl,engine,if_exists='append',index=False)
            send_success_email(succs)
        else:
            print("this DF is invalid, send failure text")
            print(errs)
            file_err.append(errs)
            send_failure_email(errs)
        #copy the processed object to archive folder.
        archive_file(mykey)


## UTILITY FUNCTIONS
def read_from_s3():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    ret = []
    for obj in my_bucket.objects.all():
        #print("My bucket object: ",obj)
        # print("key",obj.key)
        if 'archive/' not in obj.key:
            ret.append(obj.key)
    return ret

# functions to derrive values from incoming file data
def pop_prior_month_cond(s):
    # print("pop prior month in:",type(s))
    # print("pop is P1 White an int:",isinstance(s.P1_Race_White,(int,float)),type(s.P1_Race_White))
    if not isnan(s.P1_Race_White) or not isnan(s.P1_Race_Black) or not isnan(s.P1_Race_Hisp) or not isnan(s.P1_Race_Asian) or not isnan(s.P1_Race_Native) or not isnan(s.P1_Race_Pacisl) or not isnan(s.P1_Race_2Plus) or not isnan(s.P1_Race_Other) or not isnan(s.P1_Race_Unknown) or not isnan(s.P1_Race_Refused):
        return 'Yes'
    else:
        return 'No'
    
def pop_eth_sep_cond(s):
    # print("Pop eth in:",type(s))
    #print("pop eth is P2 White an int:",not isnan(s.P2_Race_White),type(s.P2_Race_White))
    if not isnan(s.P2_Race_White) or not isnan(s.P2_Race_Black) or not isnan(s.P2_Race_Hisp) or not isnan(s.P2_Race_Asian) or not isnan(s.P2_Race_Native) or not isnan(s.P2_Race_Pacisl) or not isnan(s.P2_Race_2Plus) or not isnan(s.P2_Race_Other) or not isnan(s.P2_Race_Unknown) or not isnan(s.P2_Race_Refused):
        return 'Yes'
    else:
        return 'No'
    
def adm_prior_month_cond(s):
    # print("adm YN in:",type(s))
    #print("adm can report is A1 White an int:",not isnan(s.A1_Race_White))
    if not isnan(s.A1_Race_White) or not isnan(s.A1_Race_Black) or not isnan(s.A1_Race_Hisp) or not isnan(s.A1_Race_Asian) or not isnan(s.A1_Race_Native) or not isnan(s.A1_Race_Pacisl) or not isnan(s.A1_Race_2Plus) or not isnan(s.A1_Race_Other) or not isnan(s.A1_Race_Unknown) or not isnan(s.A1_Race_Refused):
        return 'Yes'
    else:
        return 'No'

def adm_eth_sep_cond(s):
    # print("adm eth sep:",type(s))
    #print("adm eth is A2 White an int:",not isnan(s.A2_Race_White))
    if not isnan(s.A2_Race_White) or not isnan(s.A2_Race_Black) or not isnan(s.A2_Race_Hisp) or not isnan(s.A2_Race_Asian) or not isnan(s.A2_Race_Native) or not isnan(s.A2_Race_Pacisl) or not isnan(s.A2_Race_2Plus) or not isnan(s.A2_Race_Other) or not isnan(s.A2_Race_Unknown) or not isnan(s.A2_Race_Refused):
        return 'Yes'
    else:
        return 'No'

def get_formatted_datetime(s):
    return datetime.now().strftime("%m/%d/%Y %I:%M:%S")

def validate_row(row):
    resp = []
    # Month/year
    mon = int(row['Survey_Month'])
    yr = int(row['Survey_Year'])
    # Population numbers
    DetPop_First_Day = (int(row['DetPop_First_Day']) if not isnan(row['DetPop_First_Day']) else 0)
    print(not isnan(row['P1_Race_White']))
    p1w = (int(row['P1_Race_White']) if not isnan(row['P1_Race_White']) else 0) 
    p1b = (int(row['P1_Race_Black']) if not isnan(row['P1_Race_Black']) else 0)
    p1h = (int(row['P1_Race_Hisp']) if not isnan(row['P1_Race_Hisp']) else 0)
    p1a = (int(row['P1_Race_Asian']) if not isnan(row['P1_Race_Asian']) else 0)
    p1n = (int(row['P1_Race_Native']) if not isnan(row['P1_Race_Native']) else 0)
    p1p = (int(row['P1_Race_Pacisl']) if not isnan(row['P1_Race_Pacisl']) else 0 )
    p12 = (int(row['P1_Race_2Plus']) if not isnan(row['P1_Race_2Plus']) else 0)
    p1o = (int(row['P1_Race_Other']) if not isnan(row['P1_Race_Other']) else 0)
    p1u = (int(row['P1_Race_Unknown']) if not isnan(row['P1_Race_Unknown']) else 0)
    p1r = (int(row['P1_Race_Refused']) if not isnan(row['P1_Race_Refused']) else 0)
    p2w = (int(row['P2_Race_White']) if not isnan(row['P2_Race_White']) else 0)
    p2b = (int(row['P2_Race_Black']) if not isnan(row['P2_Race_White']) else 0)
    p2h = (int(row['P2_Race_Hisp']) if not isnan(row['P2_Race_Hisp']) else 0)
    p2a = (int(row['P2_Race_Asian']) if not isnan(row['P2_Race_Asian']) else 0)
    p2n = (int(row['P2_Race_Native']) if not isnan(row['P2_Race_Native']) else 0)
    p2p = (int(row['P2_Race_Pacisl']) if not isnan(row['P2_Race_Pacisl']) else 0)
    p22 = (int(row['P2_Race_2Plus']) if not isnan(row['P2_Race_2Plus']) else 0)
    p2o = (int(row['P2_Race_Other']) if not isnan(row['P2_Race_Other']) else 0)
    p2u = (int(row['P2_Race_Unknown']) if not isnan(row['P2_Race_Unknown']) else 0)
    p2r = (int(row['P2_Race_Refused']) if not isnan(row['P2_Race_Refused']) else 0)
    pop = p1w+p1b+p1h+p1a+p1n+p1p+p12+p1o+p1u+p1r+p2w+p2b+p2h+p2a+p2n+p2p+p22+p2o+p2u+p2r
    print("population",pop)
    # Admissions Count Numbers
    Total_Adm_Prior_Month = (row['Total_Adm_Prior_Month'] if not isnan(row['Total_Adm_Prior_Month']) else 0)
    a1w = (int(row['A1_Race_White']) if not isnan(row['A1_Race_White']) else 0)
    a1b = (int(row['A1_Race_Black']) if not isnan(row['A1_Race_Black']) else 0)
    a1h = (int(row['A1_Race_Hisp']) if not isnan(row['A1_Race_Hisp']) else 0)
    a1a = (int(row['A1_Race_Asian']) if not isnan(row['A1_Race_Asian']) else 0)
    a1n = (int(row['A1_Race_Native']) if not isnan(row['A1_Race_Native']) else 0)
    a1p = (int(row['A1_Race_Pacisl']) if not isnan(row['A1_Race_Pacisl']) else 0)
    a12 = (int(row['A1_Race_2Plus']) if not isnan(row['A1_Race_2Plus']) else 0)
    a1o = (int(row['A1_Race_Other']) if not isnan(row['A1_Race_Other']) else 0)
    a1u = (int(row['A1_Race_Unknown']) if not isnan(row['A1_Race_Unknown']) else 0)
    a1r = (int(row['A1_Race_Refused']) if not isnan(row['A1_Race_Refused']) else 0)
    a2w = (int(row['A2_Race_White']) if not isnan(row['A2_Race_White']) else 0)
    a2b = (int(row['A2_Race_Black']) if not isnan(row['A2_Race_Black']) else 0)
    a2h = (int(row['A2_Race_Hisp']) if not isnan(row['A2_Race_Hisp']) else 0)
    a2a = (int(row['A2_Race_Asian']) if not isnan(row['A2_Race_Asian']) else 0)
    a2n = (int(row['A2_Race_Native']) if not isnan(row['A2_Race_Native']) else 0)
    a2p = (int(row['A2_Race_Pacisl']) if not isnan(row['A2_Race_Pacisl']) else 0)
    a22 = (int(row['A2_Race_2Plus']) if not isnan(row['A2_Race_2Plus']) else 0)
    a2o = (int(row['A2_Race_Other']) if not isnan(row['A2_Race_Other']) else 0)
    a2u = (int(row['A2_Race_Unknown']) if not isnan(row['A2_Race_Unknown']) else 0)
    a2r = (int(row['A2_Race_Refused']) if not isnan(row['A2_Race_Refused']) else 0)
    adm = a1w+a1b+a1h+a1a+a1n+a1p+a12+a1o+a1u+a1r+a2w+a2b+a2h+a2a+a2n+a2p+a22+a2o+a2u+a2r
    # Admissions Reason Numbers
    aRno = (int(row['Admission_Reason_New_Offense']) if not isnan(row['Admission_Reason_New_Offense']) else 0)
    aRtc = (int(row['Admission_Reason_Technical']) if not isnan(row['Admission_Reason_Technical']) else 0)
    aRtr = (int(row['Admission_Reason_Transfer']) if not isnan(row['Admission_Reason_Transfer']) else 0)
    aRp = (int(row['Admission_Reason_Post']) if not isnan(row['Admission_Reason_Post']) else 0)
    aRo = (int(row['Admission_Reason_Other']) if not isnan(row['Admission_Reason_Other']) else 0)
    aRun = (int(row['Admission_Reason_Unknown']) if not isnan(row['Admission_Reason_Unknown']) else 0)
    aRtotal =  aRno + aRtc + aRtr + aRp + aRo + aRun
    # Validation
    if mon < 1 or mon > 12:
        resp.append("Survey_Month Invalid")
    if yr < 2000 or yr >2050:
        resp.append("Survey_Year Invalid")
    if pop != DetPop_First_Day:
        resp.append("Population counts to not match reported totals")
    if adm != Total_Adm_Prior_Month:
        resp.append("Admission counts do not match reported totals")
    if aRtotal != Total_Adm_Prior_Month:
        resp.append("Number of admission reasons")
    exi = sql_tbl.query("Survey_Year == "+str(row['Survey_Year'])+" and Survey_Month == "+str(row['Survey_Month'])+" and SiteID == '"+row['SiteID']+"'")
    print(">>>>> exists:",exi.shape)
    if exi.shape[0] > 0:
        resp.append("Row data already exists")
    return resp

def archive_file(key):
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }
    newkey = ".".join([key.split('.')[0]+datetime.now().strftime("|%m-%d-%Y %I:%M:%S"),key.split('.')[-1]])
    s3.meta.client.copy(copy_source, bucket, 'archive/'+newkey)
    s3.Object(bucket,key).delete()

def send_failure_email(errors):
    sender = "tmorris+sender@walkerinfo.com"
    recipient = "tmorris+recieve@walkerinfo.com"
    subj = "ERROR: JDAI Monthly Detention Survey Not Submitted"
    er_lst = ''
    for err in errors:
        row = str(err[0]+1)
        itmls = ", ".join(err[1])
        er_lst += "Row "+ row +" contains the error(s): "+itmls +"\n\n"
    ebody = '''Whoops! Looks like something went wrong with your JDAI Monthly Detention Survey data submission. The file you attempted to submit had the following errors:\n\n{}Please fix these errors and upload the file again. If you are still having trouble, please email jdaidata@empact.solutions\n\nThank you again for your continued participation!\nEmpact Solutions Team
    '''.format(er_lst)

    try:
        respon = ses_client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': [
                    recipient,
                ]
            },
            Message={
                'Subject': {
                    'Data': subj,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': ebody,
                        'Charset': 'UTF-8'
                    }
                }
            },
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("email sent successfully")
        print(respon['MessageId'])

def send_success_email(succs):
    print("success rows obj",succs)
    sender = "tmorris+sender@walkerinfo.com"
    recipient = "tmorris+recieve@walkerinfo.com"
    subj = "Thank you for successfully submitting the JDAI Monthly Detention Survey!"
    suc_lst = ''
    for suc in succs:
        print("individual success:",suc)
        suc_lst += suc[0] +" site added data for " + suc[1] 
    ebody = '''Thank you!\n\nYou have successfully submitted JDAI Monthly Survey data for the following site(s):\n\n{}\nWe greatly appreciate your continued participation in the Survey. We could not continue to produce robust analyses without you!\n\nBest,\nEmpact Solutions Team
    '''.format(suc_lst)

    try:
        respon = ses_client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': [
                    recipient,
                ]
            },
            Message={
                'Subject': {
                    'Data': subj,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': ebody,
                        'Charset': 'UTF-8'
                    }
                }
            },
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("email sent successfully")
        print(respon['MessageId'])