import os
import boto3
import logging
import pandas as pd
from dotenv import load_dotenv
import sqlite3
from pathlib import Path
import time
import shutil

# logging.basicConfig(filename = 'assignment_01.log',level=logging.INFO, force= True, format='%(asctime)s:%(levelname)s:%(message)s')

load_dotenv()

# LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
# logging.basicConfig(
#     format='%(asctime)s %(levelname)-8s %(message)s',
#     level=LOGLEVEL,
#     datefmt='%Y-%m-%d %H:%M:%S')

database_file_path = 'database.db'
# database_file_path = os.path.join('data/',database_file_name)

clientlogs = boto3.client('logs',
region_name= "us-east-1",
aws_access_key_id=os.environ.get('AWS_LOG_ACCESS_KEY'),
aws_secret_access_key=os.environ.get('AWS_LOG_SECRET_KEY'))


def create_connection():
    
    """AWS connnetion using boto3

    Returns:
        s3client: aws client id
    """
    
    write_logs("starting connection to s3")
    s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY1'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY1')
                        )
    write_logs("connected to s3")

    return s3client


def create_df(client_id):
    """for creatign dataframe of all the files in a particular station

    Args:
        client_id (boto3.client): aws client id
    """
    write_logs("fetching objects in NOAA s3 bucket")
    paginator = client_id.get_paginator('list_objects_v2')
    noaa_bucket = paginator.paginate(Bucket='noaa-goes18', PaginationConfig={"PageSize": 50})
    write_logs("Writing Files in list from NOAA bucket")
    station=[]
    year=[]
    day=[]
    hour=[]
    
    for count, page in enumerate(noaa_bucket):
        files = page.get("Contents")
        for file in files:
            
            if 'ABI-L1b-RadC' not in file['Key'].split("/")[0]:
                break
            
            # if ((file['Key'].split("/")[0] not in station) or (file['Key'].split("/")[1] not in year) or (file['Key'].split("/")[2] not in day) or (file['Key'].split("/")[3] not in hour)):
            station.append(file['Key'].split("/")[0])
            year.append(file['Key'].split("/")[1])
            day.append(file['Key'].split("/")[2])
            hour.append(file['Key'].split("/")[3])
            print(file['Key'])
        else:
            continue
        break
        
    df_goes18=pd.DataFrame({'Station': station,
     'Year': year,
     'Day': day,
     'Hour': hour
    })
    df_goes18.drop_duplicates(inplace=True)
    df_goes18.to_csv('data/df_goes18.csv',index=False)
    write_logs("File created in data folder")
    
    


def create_database():
    """creates data base leveraging the dataframe created
    """
    # with open(ddl_file_path, 'r') as sql_file:
        # sql_script = sql_file.read()
    db = sqlite3.connect(database_file_path)
    cursor = db.cursor()
    # cursor.executescript(sql_script)
    cursor.execute('''CREATE TABLE goes18_metadata (station text, year text, day text, hour text)''')
    goes18_dat = pd.read_csv('data/df_goes18.csv') # load to DataFrame
    goes18_dat.to_sql('goes18_metadata', db, if_exists='append', index = False) # write to sqlite table
    db.commit()
    db.close()    
 
def query_into_dataframe():
    """querying into database file for checking 
    """
    db = sqlite3.connect(database_file_path)
    df = pd.read_sql_query("SELECT * FROM goes18_metadata", db)
    write_logs(df) 
    
def check_database_initilization():
    """initializing database if not present and adding file into it
    """
    print(os.path.dirname(__file__))
    if not Path(database_file_path).is_file():
        write_logs(f"Database file not found, initilizing at : {database_file_path}")
        create_database()
        query_into_dataframe()
    else:
        write_logs("Database file already exist")
        query_into_dataframe()

def fetch_db():
    s3 = create_connection()
    bucket_name = "damg7245-team7"
    key = "database.db"
    s3.download_file(bucket_name, key, 'database.db')

    # source_file = 'database.db'
    # source_file = os.path.join('../Assignment_02', source_file)
    # destination_file = 'data/database.db'
    # destination_file = os.path.join('../Assignment_02', destination_file)

    # # Move the file to the destination directory
    # shutil.move(source_file, destination_file)


def db_connection():
    """creating a connection to database using sqlite3

    Returns:
        conn: connection to database
    """
    try:
        conn = sqlite3.connect("database.db")
        write_logs("Connected to db")    
    except Exception as e:
        print(e)
    return conn
    
    

def grab_station():
    """for pulling all the stations in the file from database

    Returns:
        stations_list: list of stations
    """
    
    conn=db_connection()
    cursor = conn.cursor()
    stations= pd.read_sql_query('SELECT distinct station FROM goes18_metadata', conn)
    stations_list=stations.Station.tolist()
    conn.close()
    
    return stations_list
    
def grab_years(station):
    """for pulling all the years in the station from database

    Args:
        station (string): station name

    Returns:
        year_list: list of all the years for a particular station
    """
    
    conn=db_connection()
    cursor = conn.cursor()
    years= pd.read_sql_query('SELECT distinct "year" FROM goes18_metadata where station="{}"'.format(station), conn)
    year_list=years.Year.tolist()
    
    conn.close()
    write_logs("Years listed for given station")
    
    return year_list

def grab_days(station,years):
    """for pulling all the days in the particular station,year from database

    Args:
        station (str): station
        years (str): year

    Returns:
        day_list: list of days for a particular station,year
    """
    
    conn=db_connection()
    cursor = conn.cursor()
    days= pd.read_sql_query('SELECT distinct "day" FROM goes18_metadata where station="{}" and "year"={}'.format(station,years), conn)
    day_list = days.Day.tolist()
    day_list=[x.zfill(3) for x in day_list]

    conn.close()
    write_logs("Days listed for given year")
    return day_list

def grab_hours(station,years,days):
    """for pulling all the hours in the file for a particular station,year,day

    Args:
        station (str): station name
        years (str): year
        days (str): day

    Returns:
        hour_list: list of all hours in the file for a particular station,year,day
    """
    
    conn=db_connection()
    cursor = conn.cursor()
    hours= pd.read_sql_query('SELECT distinct "hour" FROM goes18_metadata where station="{}" and "year"={} and "day"={}'.format(station,years,days), conn)
    hour_list=hours.Hour.tolist()
    hour_list=[x.zfill(2) for x in hour_list]
    conn.close()
    write_logs("Hours listed for given station,year,day")
    return hour_list

def grab_files(station,years,days,hours):
    """pulls files from noaa18 aws bucket for a set of station, year,day,hour

    Args:
        station (str): station name
        years (str): year
        days (str): day
        hours (str): hour

    Returns:
        file_names: list of files present in noaa18 aws bucket for a set of station, year,day,hour
    """
    
    client_id=create_connection()
    
    write_logs("fetching Files in list from NOAA bucket")
        
    file_names = []
    bucket = 'noaa-goes18'
    result = client_id.list_objects(Bucket=bucket, Prefix= station + "/" + years + "/" + days + "/" + hours + "/", Delimiter='/')
    for o in result.get('Contents'):
        file_names.append(o.get('Key').split('/')[4])

    write_logs("files retrieved for the given year, month, day and station from the S3 bucket")
    
    return file_names
        
def create_url(station,year,day,hour,file_name):
    """generates noaa18 aws bucket url based on station, year, day, hour, file name

    Args:
        station (str): station name
        year (str): year
        day (str): day
        hour (str): hour
        file_name (str): file name

    Returns:
        url: url of the selected file
    """
       
    url= 'https://noaa-goes18.s3.amazonaws.com/' + station + '/'+year + '/'+ day + '/'+ hour + '/'+ file_name
    
    write_logs("URL created for NOAA-GOES18 bucket")
    write_logs(url)
    return url

def generate_key(station,year,day,hour,file_name):
    """generates key for a particular selected file 

    Args:
        station (str): station name
        year (str): year
        day (str): day
        hour (str): hour
        file_name (str): file name

    Returns:
        key: key for accessing the selected file aws bucket
    """
    
    key = station + '/' + year + '/' + day + '/' + hour + '/' + file_name
    
    return key

def copy_files_s3(key,filename):
    """copies file between two aws buckets ie from noaa18 to my aws bucket (damg7245-demo)

    Args:
        key (str): key for a file
        filename (str): name of the file

    Returns:
        url: returns url of file in smy bucket
    """
    
    client_id=create_connection()

    client_id.copy_object(Bucket='damg7245-team7' , CopySource={"Bucket": 'noaa-goes18', "Key": key}, Key=filename)
    
    url='https://damg7245-demo.s3.amazonaws.com/' + filename
    write_logs("URL created for Personal bucket")
    write_logs(url)
    
    return url


def write_logs(message):
    
    """Writes the logs to the cloudwatch logs

    Args:
        message (str): The message to be written to the logs
    """
    
    clientlogs.put_log_events (
    logGroupName="assignment_01",
    logStreamName="app_logs",
    logEvents=[
        {
    'timestamp' : int(time.time()* 1e3),
    'message': message,
    }
    ]
    )

# for creating dataframe and data base follow below code

# def main():
    # client_id=create_connection()
    # create_df(client_id)
    # check_database_initilization()



# if __name__ == "__main__":
#     write_logs("Script starts")
#     main()
#     write_logs("Script ends")
        