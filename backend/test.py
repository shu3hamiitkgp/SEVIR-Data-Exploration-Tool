
import os
import boto3
import logging
import pandas as pd
from dotenv import load_dotenv
import sqlite3
from pathlib import Path

load_dotenv()

s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

# bucket = 'noaa-goes18'
# # station_ref='ABI-L1b-RadC'
# files = s3client.list_objects(Bucket=bucket)

# file_names = []
# for o in files.get('Contents'):
#     if o.get('Key').split('/')[0]=='ABI-L1b-RadC':
#         file_names.append(o.get('Key').split('/')[4])

# print(file_names,len(file_names))

def create_df(client_id):
    """for creatign dataframe of all the files in a particular station

    Args:
        client_id (boto3.client): aws client id
    """
    logging.debug("fetching objects in NOAA s3 bucket")
    paginator = client_id.get_paginator('list_objects_v2')
    noaa_bucket = paginator.paginate(Bucket='noaa-goes18', PaginationConfig={"PageSize": 50})
    logging.info("Writing Files in list from NOAA bucket")
    station=[]
    year=[]
    day=[]
    hour=[]
    
    # file_names = []
    bucket = 'noaa-goes18'
    # station_ref='ABI-L1b-RadC'
    files = client_id.list_objects(Bucket=bucket,)
    
    for file in files.get('Contents'):
        if file.get('Key').split('/')[0]=='ABI-L1b-RadC':
            station.append(file['Key'].split("/")[0])
            year.append(file['Key'].split("/")[1])
            day.append(file['Key'].split("/")[2])
            hour.append(file['Key'].split("/")[3])
            print(file['Key'])
        
    df_goes18=pd.DataFrame({'Station': station,
     'Year': year,
     'Day': day,
     'Hour': hour
    })
    df_goes18.drop_duplicates(inplace=True)
    df_goes18.to_csv('df_goes18.csv',index=False)    
 

    
def main():
    create_df(s3client)
    # query_into_dataframe()


if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")
        

# bucket = 'noaa-goes18'

# result=s3client.list_objects(Bucket=bucket, Prefix= 'ABI-L1b-RadC' + "/" + '2022' + "/" + '209' + "/" + '00' + "/", Delimiter='/')

# file_names = []
# for o in result.get('Contents'):
#     file_names.append(o.get('Key').split('/')[4])

# print(file_names)


# import sqlite3

# def db_connection():
#     try:
#         conn = sqlite3.connect("data/goes18.db")    
#     except Exception as e:
#         print(e)
#     return conn


# def grab_days(station,years):
    
#     conn=db_connection()
#     cursor = conn.cursor()
#     days= pd.read_sql_query('SELECT distinct "day" FROM goes18_metadata where station="{}" and "year"={}'.format(station,years), conn)
#     day_list = days.day.tolist()

#     day_list=[x.zfill(3) for x in day_list]
#     # day_list=["00{}".format(x) if x<10 else '0{}'.format(x) if x<100 else str(x) for x in day_list]

#     conn.close()
    
#     return day_list

# def grab_hours(station,years,days):
    
#     conn=db_connection()
#     cursor = conn.cursor()
#     hours= pd.read_sql_query('SELECT distinct "hour" FROM goes18_metadata where station="{}" and "year"={} and "day"={}'.format(station,years,days), conn)
#     hour_list=hours.hour.tolist()
#     hour_list=[x.zfill(2) for x in hour_list]
#     # hour_list=["0{}".format(x) if x<10 else str(x) for x in hour_list]
#     conn.close()
    
#     return hour_list


# days=grab_days('ABI-L1b-RadC','2023')
# hours=grab_hours('ABI-L1b-RadC','2023','1')

# print(hours)
