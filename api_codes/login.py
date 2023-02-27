from backend import schema
from fastapi import FastAPI,Depends
import sqlite3
import os
import pandas as pd
from passlib.context import CryptContext
from backend import access_token
from backend import oauth2
from fastapi.security import OAuth2PasswordRequestForm
import re
import requests
import boto3
import logging
import time

app = FastAPI()

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
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )
    write_logs("connected to s3")

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

@app.post("/validatefileUrl")
def validate_file(filename: str,token_data: schema.TokenData = Depends(oauth2.get_current_user)):
  write_logs("Entered file validation function")
  products = ['ABI-L1b-RadF', 'ABI-L1b-RadC', 'ABI-L1b-RadM', 'ABI-L2-ACHAC','ABI-L2-ACHAF','ABI-L2-ACHAM','ABI-L2-ACHTF',
             'ABI-L2-ACHTM','ABI-L2-ACMC','ABI-L2-ACMF','ABI-L2-ACMM','ABI-L2-ACTPC','ABI-L2-ACTPM','ABI-L2-ACTPF','ABI-L2-ADPM','ABI-L2-AICEF',
             'ABI-L2-AITAF','ABI-L2-AODC','ABI-L2-AODF','ABI-L2-BRFC','ABI-L2-BRFF','ABI-L2-BRFM','ABI-L2-CMIPC','ABI-L2-CMIPF',
             'ABI-L2-CMIPM','ABI-L2-CODC','ABI-L2-CODF','ABI-L2-CPSC','ABI-L2-CPSF','ABI-L2-CPSM','ABI-L2-CTPC','ABI-L2-CTPF','ABI-L2-DMWC',
             'ABI-L2-DMWF','ABI-L2-DMWM','ABI-L2-DMWVC','ABI-L2-DMWVF','ABI-L2-DMWVM','ABI-L2-DSIC','ABI-L2-DSIF','ABI-L2-DSIM','ABI-L2-DSRC',
             'ABI-L2-DSRF','ABI-L2-DSRM','ABI-L2-FDCC','ABI-L2-FDCF','ABI-L2-FDCM','ABI-L2-LSAC','ABI-L2-LSAF','ABI-L2-LSAM','ABI-L2-LSTC',
             'ABI-L2-LSTF','ABI-L2-LSTM','ABI-L2-LVMPC','ABI-L2-LVMPF','ABI-L2-LVMPM','ABI-L2-LVTPC','ABI-L2-LVTPF','ABI-L2-LVTPM','ABI-L2-MCMIPC',
             'ABI-L2-MCMIPF','ABI-L2-MCMIPM','ABI-L2-RRQPEF','ABI-L2-RSRC','ABI-L2-RSRF','ABI-L2-SSTF','ABI-L2-TPWC','ABI-L2-TPWF','ABI-L2-TPWM',
             'ABI-L2-VAAF','EXIS-L1b-SFEU','EXIS-L1b-SFXR','GLM-L2-LCFA','MAG-L1b-GEOF','SEIS-L1b-EHIS','SEIS-L1b-MPSH','SEIS-L1b-MPSL','SEIS-L1b-SGPS',
             'SUVI-L1b-Fe093','SUVI-L1b-Fe131','SUVI-L1b-Fe171','SUVI-L1b-Fe195','SUVI-L1b-Fe284','SUVI-L1b-He303']
  
  productLengths = []

  for product in products:
    productLen = len(product)
    if productLen not in productLengths:
      productLengths.append(productLen)
  
  file_name_split = filename.split('_')

  if(len(file_name_split)>6):
    write_logs("File name is invalid")
    write_logs("File validation function execution complete")
    data = {'message': 'File name is invalid','status_code': '200'}
    return data

  if file_name_split[0] != 'OR':
    write_logs("File name should start with OR")
    write_logs("File validation function execution complete")
    data = {'message': 'File name should start with OR', 'status_code': '200'}
    return data

  x=0
  for i in productLengths:
    if len(file_name_split[1])>=i:
      if file_name_split[1][0:i-1] in products:
        x=1
        
  if x!=1:
    write_logs("Product name specified is invalid")
    write_logs("File validation function execution complete")
    data = {'message': 'Product name specified is invalid', 'status_code': '200'}
    return data
  
  productVersion = file_name_split[1].split('-')[-1:][0]
  if len(productVersion)==2:
    if productVersion[0]!='M' or (not productVersion[1].isnumeric()):
      write_logs("Mode or channel number error")
      write_logs("File validation function execution complete")
      data = {'message': 'Mode or channel number error', 'status_code': '200'}
      return data
  elif len(productVersion)==5:
    if productVersion[0]!='M' or productVersion[2]!='C' or (not productVersion[1].isnumeric()) or (not productVersion[3:5].isnumeric()):
      write_logs("Mode or channel number error")
      write_logs("File validation function execution complete")
      data = {'message': 'Mode or channel number error', 'status_code': '200'}
      return data
  else:
    write_logs("Mode or channel number error")
    write_logs("File validation function execution complete")
    data = {'message': 'Mode or channel number error', 'status_code': '200'}
    return data

  if file_name_split[2][0]!='G' or (not file_name_split[2][1:3].isnumeric()):
    write_logs("Goes number specification error")
    write_logs("File validation function execution complete")
    data = {'message': 'Goes number specification error', 'status_code': '200'}
    return data
  elif file_name_split[2][1:3].isnumeric():
    if int(file_name_split[2][1:3])>18:
      write_logs("Goes number specification error")
      write_logs("File validation function execution complete")
      data = {'message': 'Goes number specification error', 'status_code': '200'}
      return data

  if len(file_name_split[3])==15 and file_name_split[3][0]=='s' and file_name_split[3][1:].isnumeric():
    if int(file_name_split[3][1:5])>2023 or int(file_name_split[3][5:8])>366 or int(file_name_split[3][8:10])>24 or int(file_name_split[3][10:12])>60 or int(file_name_split[3][12:14])>60:
      write_logs("Scan time specified is in incorrect format")
      write_logs("File validation function execution complete")
      data = {'message': 'Scan time specified is in incorrect format', 'status_code': '200'}
      return data
  else:
    write_logs("Scan time specified is in incorrect format")
    write_logs("File validation function execution complete")
    data = {'message': 'Scan time specified is in incorrect format', 'status_code': '200'}
    return data

  if len(file_name_split[4])==15 and file_name_split[4][0]=='e' and file_name_split[4][1:].isnumeric():
    if int(file_name_split[4][1:5])>2023 or int(file_name_split[4][5:8])>366 or int(file_name_split[4][8:10])>24 or int(file_name_split[4][10:12])>60 or int(file_name_split[4][12:14])>60:
      write_logs("End time specified is in incorrect format")
      write_logs("File validation function execution complete")
      data = {'message': 'End time specified is in incorrect format', 'status_code': '200'}
      return data
  else:
    write_logs("End time specified is in incorrect format")
    write_logs("File validation function execution complete")
    data = {'message': 'End time specified is in incorrect format', 'status_code': '200'}
    return data

  file_name_split[5]=file_name_split[5].replace(" ","")
  if file_name_split[5][15:18]!='.nc':
    data = {'message': 'File name should end with .nc only', 'status_code': '200'}
    return data
  
  if len(file_name_split[5])==18 and file_name_split[5][0]=='c' and file_name_split[5][1:15].isnumeric() and file_name_split[5][15:18]=='.nc':
    if int(file_name_split[5][1:5])>2023 or int(file_name_split[5][5:8])>366 or int(file_name_split[5][8:10])>24 or int(file_name_split[5][10:12])>60 or int(file_name_split[5][12:14])>60:
      write_logs("File creation time specified is in incorrect format")
      write_logs("File validation function execution complete")
      data = {'message': 'File creation time specified is in incorrect format', 'status_code': '200'}
      return data
  else:
    write_logs("File creation time specified is in incorrect format")
    write_logs("File validation function execution complete")
    data = {'message': 'File creation time specified is in incorrect format', 'status_code': '200'}
    return data

  write_logs("Valid filename")
  write_logs("File validation function execution complete")
  data = {'message': 'Valid filename', 'status_code': '200'}
  return data

@app.post("/login")
async def read_root(login_data: OAuth2PasswordRequestForm = Depends()):
    try:
        database_file_name = "assignment_01.db"
        database_file_path = os.path.join('data/',database_file_name)
        db = sqlite3.connect(database_file_path)
        user= pd.read_sql_query('SELECT * FROM Users where username="{}"'.format(login_data.username), db)
        if len(user) == 0:
           data = {"message": "User not found", "status_code": "404"}
        else:
            pwd_cxt = CryptContext(schemes=["bcrypt"], deprecated="auto")
            if pwd_cxt.verify(login_data.password, user['hashed_password'][0]):
                data = {'message': 'Username verified successfully', 'status_code': '200'}
                accessToken = access_token.create_access_token(data={"sub": user.username})
            else:
                data = {'message': 'Password is incorrect','status_code': '401'}
    except Exception as e:
        data = {'message': str(e),'status_code': '500'}
    return data

@app.post("/getfileUrl")
async def getFileUrl(filename: str,token_data: schema.TokenData = Depends(oauth2.get_current_user)):
    write_logs("Get file url function entered")

    base_url="https://noaa-goes"+filename.split('_')[2][1:3]+".s3.amazonaws.com/"
    timestamp = filename.split('_')[-3].split(".")[0]
    timestamp

    products = ['ABI-L1b-RadF', 'ABI-L1b-RadC', 'ABI-L1b-RadM', 'ABI-L2-ACHAC','ABI-L2-ACHAF','ABI-L2-ACHAM','ABI-L2-ACHTF',
                'ABI-L2-ACHTM','ABI-L2-ACMC','ABI-L2-ACMF','ABI-L2-ACMM','ABI-L2-ACTPC','ABI-L2-ACTPM','ABI-L2-ACTPF','ABI-L2-ADPM','ABI-L2-AICEF',
                'ABI-L2-AITAF','ABI-L2-AODC','ABI-L2-AODF','ABI-L2-BRFC','ABI-L2-BRFF','ABI-L2-BRFM','ABI-L2-CMIPC','ABI-L2-CMIPF',
                'ABI-L2-CMIPM','ABI-L2-CODC','ABI-L2-CODF','ABI-L2-CPSC','ABI-L2-CPSF','ABI-L2-CPSM','ABI-L2-CTPC','ABI-L2-CTPF','ABI-L2-DMWC',
                'ABI-L2-DMWF','ABI-L2-DMWM','ABI-L2-DMWVC','ABI-L2-DMWVF','ABI-L2-DMWVM','ABI-L2-DSIC','ABI-L2-DSIF','ABI-L2-DSIM','ABI-L2-DSRC',
                'ABI-L2-DSRF','ABI-L2-DSRM','ABI-L2-FDCC','ABI-L2-FDCF','ABI-L2-FDCM','ABI-L2-LSAC','ABI-L2-LSAF','ABI-L2-LSAM','ABI-L2-LSTC',
                'ABI-L2-LSTF','ABI-L2-LSTM','ABI-L2-LVMPC','ABI-L2-LVMPF','ABI-L2-LVMPM','ABI-L2-LVTPC','ABI-L2-LVTPF','ABI-L2-LVTPM','ABI-L2-MCMIPC',
                'ABI-L2-MCMIPF','ABI-L2-MCMIPM','ABI-L2-RRQPEF','ABI-L2-RSRC','ABI-L2-RSRF','ABI-L2-SSTF','ABI-L2-TPWC','ABI-L2-TPWF','ABI-L2-TPWM',
                'ABI-L2-VAAF','EXIS-L1b-SFEU','EXIS-L1b-SFXR','GLM-L2-LCFA','MAG-L1b-GEOF','SEIS-L1b-EHIS','SEIS-L1b-MPSH','SEIS-L1b-MPSL','SEIS-L1b-SGPS',
                'SUVI-L1b-Fe093','SUVI-L1b-Fe131','SUVI-L1b-Fe171','SUVI-L1b-Fe195','SUVI-L1b-Fe284','SUVI-L1b-He303']
    
    for prod in products:
        if prod in filename.split('_')[1]:
            file_type = prod

    year = timestamp[1:5]
    day_of_the_year = timestamp[5:8]
    time_of_day = timestamp[8:10]
    final_url = base_url + file_type + '/' + year + '/' + day_of_the_year + '/' + time_of_day + '/' + filename

    try:
        # Make a GET request to the URL
        response = requests.get(final_url)

        # Check if the response was successful
        if response.status_code == 200:
            write_logs("File url: "+ final_url +" downloaded")
            write_logs("Get file url function complete")
            data = {'message': final_url,'status_code': '200'}
            return data
        else:
            write_logs("File name not found")
            write_logs("Get file url function complete")
            data = {'message': 'File name not found','status_code': '404'}
            return data

    except Exception as e:
        write_logs("Get file url function throws error")
        write_logs("Get file url function complete")
        data = {'message': 'An error occured while retriving the file','status_code': '500'}
        return data