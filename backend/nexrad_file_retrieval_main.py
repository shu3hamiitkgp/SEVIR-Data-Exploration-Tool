import re
import os
import time
import boto3
import logging
import requests
from datetime import datetime


# base_goes_url = "https://noaa-goes18.s3.amazonaws.com/"
base_nexrad_url = "https://noaa-nexrad-level2.s3.amazonaws.com/"
format = "%Y%m%d%H%M%S"
pattern = "^(?:[A-Z]{4}\d{8}|[A-Z]{3}\d{9})_\d{6}(?:_V\d{2}\.gz|_V\d{2}_MDM|_V\d{2}|\.gz)$"
clientlogs = boto3.client('logs',
region_name= "us-east-1",
aws_access_key_id=os.environ.get('AWS_LOG_ACCESS_KEY'),
aws_secret_access_key=os.environ.get('AWS_LOG_SECRET_KEY'))


def get_nexrad_file_url(filename):

  # write_logs("NEXRAD_File_Link_Retrieval: Inside get_nexrad_file_url function")

  if re.match(pattern, filename):
    write_logs("NEXRAD_File_Link_Retrieval: Entered file name matches pattern")
    date_time_val = filename.split('_')[0][4:12]+filename.split('_')[1][0:6]

    if date_time_format(date_time_val):
      write_logs("NEXRAD_File_Link_Retrieval: Entered file name matches date/time format")
      year = filename.split('_')[0][4:8]
      month_of_year = filename.split('_')[0][8:10]
      day_of_month = filename.split('_')[0][10:12]
      ground_station_id = filename.split('_')[0][0:4]
      # timestamp = filename.split('_')[1]

      final_url = base_nexrad_url + year + '/' + month_of_year + '/' + day_of_month + '/' + ground_station_id + '/' + filename
      # print(final_url)
      write_logs("NEXRAD_File_Link_Retrieval: URL Generated: " + final_url)

      try:
        # Make a GET request to the URL
        response = requests.get(final_url)
        # write_logs("NEXRAD_File_Link_Retrieval: GET request to generated URL " + final_url)
        # Check if the response was successful
        if response.status_code == 200:
          write_logs("NEXRAD_File_Link_Retrieval: GET request to generated URL - Response Successful")
          write_logs("NEXRAD_File_Link_Retrieval: Download link generated for entered file name: " + filename)
          return final_url
        else:
          write_logs("NEXRAD_File_Link_Retrieval: GET request to generated URL - Error Response: " + str(response.status_code))
          return response.status_code         

      except Exception as e:
        write_logs("NEXRAD_File_Link_Retrieval: GET request to generated URL - Exception: " + str(e))
        return e

    else:
      write_logs("NEXRAD_File_Link_Retrieval: Entered file name date/time format invalid")
      return 'invalid datetime'

  else:
    write_logs("NEXRAD_File_Link_Retrieval: Entered file name invalid")
    return 'invalid filename'


def create_connection():
    
    """AWS connnetion using boto3

    Returns:
        s3client: aws client id
    """
    
    write_logs("NEXRAD_File_Link_Retrieval: Starting Connection to S3")
    s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )
    write_logs("NEXRAD_File_Link_Retrieval: Connected to S3")

    return s3client

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

def date_time_format(val):

  res = True
  
  try:
    res = bool(datetime.strptime(val, format))
  except ValueError:
    res = False
  return res
  