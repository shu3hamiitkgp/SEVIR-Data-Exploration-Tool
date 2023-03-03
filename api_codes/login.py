from backend import schema
from fastapi import FastAPI,Depends, APIRouter
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
import sys

cwd = os.getcwd()
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, project_dir)
# os.environ['PYTHONPATH'] = project_dir + ':' + os.environ.get('PYTHONPATH', '')

# app = FastAPI()

router = APIRouter()

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


@router.post("/login")
async def read_root(login_data: OAuth2PasswordRequestForm = Depends()):
    # try:
    database_file_name = "assignment_01.db"
    database_file_path = os.path.join(project_dir, os.path.join('data/',database_file_name))
    db = sqlite3.connect(database_file_path)
    user= pd.read_sql_query('SELECT * FROM Users where username="{}"'.format(login_data.username), db)
    if len(user) == 0:
        data = {"message": "User not found", "status_code": "404"}
    else:
        pwd_cxt = CryptContext(schemes=["bcrypt"], deprecated="auto")
        if pwd_cxt.verify(login_data.password, user['hashed_password'][0]):
            print("password verified")
            data = {'message': 'Username verified successfully', 'status_code': '200'}
            accessToken = access_token.create_access_token(data={"sub": str(user['username'][0])})
            data = {'message': "Success",'access_token':accessToken,'status_code': '200'}
        else:
            data = {'message': 'Password is incorrect','status_code': '401'}
    # except Exception as e:
    #     data = {'message': str(e),'status_code': '500'}
    return data

