import ast
import time
import pandas as pd
import os
import boto3
from dotenv import load_dotenv
import json
import random, string
import logging
import sqlite3
import sys
import shutil


cwd = os.getcwd()


# logging.basicConfig(filename = 'assignment_01.log',level=logging.INFO, force= True, format='%(asctime)s:%(levelname)s:%(message)s')


load_dotenv()

# Navigate to the project directory
project_dir = os.path.abspath(os.path.join(cwd, ''))
sys.path.insert(0, project_dir)
os.environ['PYTHONPATH'] = project_dir + ':' + os.environ.get('PYTHONPATH', '')

database_path = os.path.join(project_dir, 'data' , 'database.db')




clientlogs = boto3.client('logs',
region_name= "us-east-1",
aws_access_key_id=os.environ.get('AWS_LOG_ACCESS_KEY'),
aws_secret_access_key=os.environ.get('AWS_LOG_SECRET_KEY'))


def fetch_db():

    print("Inside fetch_db")


    s3 = createConnection()
    bucket_name = "damg7245-team7"
    key = "database.db"
    s3.download_file(bucket_name, key, 'database.db')

    # source_file = 'database.db'
    # source_file = os.path.join(project_dir, source_file)
    # destination_file = 'data/database.db'
    # destination_file = os.path.join(project_dir, destination_file)

    # # Move the file to the destination directory
    # shutil.move(source_file, destination_file)




def createConnection():
    
    """ This function creates a connection to the AWS S3 bucket for fetching data
    Args:
        None
    Returns:
        s3client (boto3.client): The boto3 client object
    """


    s3client = boto3.client('s3',
    region_name= "us-east-1",
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY1'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_KEY1'))

    write_logs("Connection to S3 bucket created")

    return s3client



def get_distinct_month(yearSelected):

    """This function fetches the distinct months from the database

    Args:
        yearSelected (string): The year selected by the user

    Returns:
        month (list): The list of months
    """

    connection = sqlite3.connect('database.db')
    cursor = connection.cursor()
    month = pd.read_sql_query("SELECT DISTINCT Month FROM nexrad_" + yearSelected, connection)
    month = month['Month'].tolist()
    month.insert(0, None)
    return month

def get_distinct_day(yearSelected, monthSelected):

    """This function fetches the distinct days from the database
    
    Args:
        yearSelected (string): The year selected by the user
        monthSelected (string): The month selected by the user
        
    Returns:
        day (list): The list of days
    """
    
    connection = sqlite3.connect('database.db')

    cursor = connection.cursor()
    day = pd.read_sql_query("SELECT DISTINCT Day FROM nexrad_" + yearSelected + " WHERE year = " + yearSelected + " AND Month = " + monthSelected, connection)
    day = day['Day'].tolist()
    day.insert(0, None)
    return day

def get_distinct_station(yearSelected, monthSelected, daySelected):

    """This function fetches the distinct stations from the database

    Args:
        yearSelected (string): The year selected by the user
        monthSelected (string): The month selected by the user
        daySelected (string): The day selected by the user
    
    Returns:
        station (list): The list of stations
    """
        
    connection = sqlite3.connect('database.db')

    cursor = connection.cursor()
    station = pd.read_sql_query("SELECT DISTINCT Station FROM nexrad_" + yearSelected + " where year = " + yearSelected + " and month = " + monthSelected + " and day = " + daySelected, connection)
    station = station['Station'].tolist()
    station.insert(0, None)
    return station
    


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











