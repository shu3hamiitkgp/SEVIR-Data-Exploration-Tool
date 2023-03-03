from backend import schema
from fastapi import FastAPI,Depends,Response,status
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
from datetime import datetime,timedelta


project_dir = os.getcwd()
sys.path.insert(0, project_dir)
os.environ.get('PYTHONPATH', '')
os.environ['PYTHONPATH'] = project_dir + ':' + os.environ.get('PYTHONPATH', '')


from backend import main_goes18, goes_file_retrieval_main
from backend import main_goes18, goes_file_retrieval_main, nexrad_file_retrieval_main
from pydantic import BaseModel
import random
import string
from backend import nexrad_main

from api_codes.goes_api import router as goes_router
from api_codes.nexrad_api import router as nexrad_router
from api_codes.s3_api import router as s3_router
from api_codes.login import router as login_router

app = FastAPI()

app.include_router(goes_router)
app.include_router(nexrad_router)
app.include_router(s3_router)
app.include_router(login_router)


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

@app.get('/is_logged_in')
async def is_logged_in(getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    return {'status_code': '200'}




@app.post('/create_default_user')
async def create_default_user():
    database_file_name = "assignment_01.db"
    database_file_path = os.path.join(project_dir, os.path.join('data/',database_file_name))
    db = sqlite3.connect(database_file_path)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE if not exists Users (username,hashed_password,service_plan,api_limit)''')
    user= pd.read_sql_query("SELECT * FROM Users", db)
    if len(user) == 0:
        pwd_cxt = CryptContext(schemes=["bcrypt"], deprecated="auto")
        hashed_password = pwd_cxt.hash(("spring2023"))
        cursor.execute("Insert into Users values (?,?,?,?)", ("user1",hashed_password,"Free",10))
        cursor.execute("Insert into Users values (?,?,?,?)", ("user2",hashed_password,"Gold",15))
        cursor.execute("Insert into Users values (?,?,?,?)", ("user3",hashed_password,"Platinum",20))
        db.commit()
        db.close()
    return {'status_code': '200'}




@app.post('/update_login')
async def login_update(getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    database_file_name = "assignment_01.db"
    database_file_path = os.path.join(project_dir, os.path.join('data/',database_file_name))
    db = sqlite3.connect(database_file_path)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE if not exists Logins (username,logindate)''')
    cursor.execute("Insert into Logins values (?,?)", (getCurrentUser.username,datetime.utcnow()))
    db.commit()
    db.close()
    return {'status_code': '200'}

@app.post('/user_api_status')
async def get_user_data(api_details: schema.api_detail_fetch,getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    database_file_name = "assignment_01.db"
    database_file_path = os.path.join(project_dir, os.path.join('data/',database_file_name))
    db = sqlite3.connect(database_file_path)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE if not exists user_activity (username,service_plan,api_limit,date,api_name,hit_count)''')
    cursor.execute('SELECT * FROM user_activity WHERE username =? ORDER BY date DESC LIMIT 1',(getCurrentUser.username,))
    result = cursor.fetchone()
    username=getCurrentUser.username
    api_limit=pd.read_sql_query('Select api_limit from Users where username="{}"'.format(username),db).api_limit.item()
    date = datetime.utcnow()
    service_plan=pd.read_sql_query('Select service_plan from Users where username="{}"'.format(username),db).service_plan.item()
    api_name=api_details.api_name 
    if not result:
        hit_count = 1
        cursor.execute('INSERT INTO user_activity VALUES (?,?,?,?,?,?)', (username,service_plan,api_limit,date,api_name,hit_count))
        db.commit()
    else:
        last_date = datetime.strptime(result[3], '%Y-%m-%d %H:%M:%S.%f')
        time_diff = datetime.utcnow() - last_date
        if time_diff <= timedelta(hours=1):
            if result[5]<api_limit:
                hit_count = result[5] + 1
                cursor.execute('INSERT INTO user_activity VALUES (?,?,?,?,?,?)', (username,service_plan,api_limit,date,api_name,hit_count))
                db.commit()
            else:
                db.commit()
                db.close() 
                return Response(status_code=status.HTTP_429_TOO_MANY_REQUESTS)
        else:
            hit_count = 1
            cursor.execute('INSERT INTO user_activity VALUES (?,?,?,?,?,?)', (username,service_plan,api_limit,date,api_name,hit_count))
            db.commit()



@app.post("/signup")
async def signup(user_data: schema.User):
    database_file_name = "assignment_01.db"
    database_file_path = os.path.join(project_dir, os.path.join('data/',database_file_name))
    db = sqlite3.connect(database_file_path)
    user = pd.read_sql_query('SELECT * FROM Users where username="{}"'.format(user_data.username), db)
    if len(user) > 0:
           return {'message': 'Username already exists','status_code': '409'}
    cursor = db.cursor()
    pwd_cxt = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hashed_password = pwd_cxt.hash((user_data.password))
    cursor.execute("Insert into Users values (?,?,?,?)",
                   (user_data.username, hashed_password, user_data.service_plan, user_data.api_limit))
    db.commit()
    db.close()
    return {'status_code': '200'}    

@app.get("/get_useract_data")
async def useract_data(getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    database_file_name = "assignment_01.db"
    database_file_path = os.path.join('data/',database_file_name)
    db = sqlite3.connect(database_file_path)
    try:
        df=pd.read_sql_query('Select * from user_activity where username="{}"'.format(getCurrentUser.username),db)
        df_json=df.to_dict(orient='records')
        db.close()
        return {'data':df_json}
    except:
        return {'data':'No data found'}

@app.post("/get_current_username")
async def get_username(getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    
    # print(getCurrentUser)
    
    return {'username': getCurrentUser.username}