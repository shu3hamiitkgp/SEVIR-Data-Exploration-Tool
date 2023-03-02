import datetime as dt
from fastapi import Response, status
import requests
import boto3
import os
from dotenv import load_dotenv
import sqlite3
from pathlib import Path
import pandas as pd
import sys
import bcrypt
import typer


env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

cwd = os.getcwd()
project_dir = os.path.abspath(os.path.join(cwd, '..'))
sys.path.insert(0, project_dir)
os.environ['PYTHONPATH'] = project_dir + ':' + os.environ.get('PYTHONPATH', '')


from api_codes import nexrad_api, s3_api
from backend import nexrad_main


database_path = os.path.join(project_dir, os.path.join('data', 'assignment_03.db'))
app = typer.Typer()


def create_db():

    """
    Create a table users in the database


    """
     
    db = sqlite3.connect(database_path)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE if not exists Users (username,hashed_password, plan, api_limit)''')
    db.commit()
    db.close()

def insert_user(username, password, plan, api_limit):

    """
    Insert a user into the database

    Args:
        username (str): User name
        password (str): Password
        plan (int): User plan
        api_limit (int): API limit
    
    Returns:
        None
    """

    db = sqlite3.connect(database_path)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE if not exists Users (username,hashed_password, plan, api_limit)''')
    cursor.execute('INSERT INTO Users VALUES (?,?,?,?)', (username, password, plan, api_limit))
    db.commit()
    db.close()



def user_status(username:str, api_name: str):
    """
    Check the user status if they have exceeded the api limit

    Args:
        username (str): User name
        api_name (str): API name

    Returns:
        None
    """

    db = sqlite3.connect(database_path)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE if not exists user_activity (username,api_limit,date,api_name,hit_count)''')
    cursor.execute('SELECT * FROM user_activity WHERE username =? ORDER BY date DESC LIMIT 1',(username,))
    result = cursor.fetchone()
    api_limit=pd.read_sql_query('Select api_limit from Users where username="{}"'.format(username),db).api_limit.item()
    date = dt.datetime.utcnow()
    if not result:
        hit_count = 1
        cursor.execute('INSERT INTO user_activity VALUES (?,?,?,?,?)', (username,api_limit,date,api_name,hit_count))
        db.commit()
    else:
        last_date = dt.datetime.strptime(result[2], '%Y-%m-%d %H:%M:%S.%f')
        time_diff = dt.datetime.utcnow() - last_date
        # time_diff = time_diff + dt.timedelta(hours=1)
        if time_diff <= dt.timedelta(hours=1):
            if result[4]<api_limit:
                hit_count = result[4] + 1
                cursor.execute('INSERT INTO user_activity VALUES (?,?,?,?,?)', (username,api_limit,date,api_name,hit_count))
                db.commit()
            else:
                db.commit()
                db.close() 
                return "Too many requests wait for 1 hour"
        else:
            hit_count = 1
            cursor.execute('INSERT INTO user_activity VALUES (?,?,?,?,?)', (username,api_limit,date,api_name,hit_count))
            db.commit()
            # db.close()






def create_connection():

    """
    Create a connection to AWS S3 bucket

    Returns:
        s3client: boto3 client object

    """

    s3client = boto3.client('s3',
    region_name= "us-east-1",
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY1'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_KEY1'))

    return s3client




@app.command()
def createuser(username: str):
    """ 
    Create a user in the system

    Args:
        username (str): User name

    Returns:
        None
    """


    password = typer.prompt("Enter password", hide_input=True)
    confirm_password = typer.prompt("Confirm password", hide_input=True)

    if password != confirm_password:
        typer.echo("Passwords do not match")
        return
    
    if password == "":
        typer.echo("Password cannot be empty")
        return
    
    if password == confirm_password:
        user_tier = typer.prompt("Select the tier you want to use \n 1. Free \n 2. Gold \n 3. Platium", type=int)
        if user_tier not in [1, 2, 3]:
            typer.echo("Invalid tier selection")
            return
        
        password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()
        create_db()
        users = pd.read_sql_query("SELECT * FROM Users", connection)
        user_lst = users["username"].tolist()

        if username in user_lst:
            typer.echo(f"User {username} already exists")
            return
        
        else:
            if user_tier == 1:
                api_limit = 10

            if user_tier == 2:
                api_limit = 15
            
            if user_tier == 3:
                api_limit = 20
    
            insert_user(username, password, user_tier, api_limit)
            typer.echo(f"User {username} created successfully")


@app.command()
def fetchnexrad(username: str, password: str):
               
    """
    Fecth nexrad data from S3 bucket

    Args:
        username (str): User name
        password (str): Password

    Returns:
        None
    """

    s3client = create_connection()
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM Users", connection)
    user_lst = users["username"].tolist()

    if username not in user_lst:
        typer.echo(f"User {username} does not exist")
        return

    stored_password = pd.read_sql("SELECT hashed_password FROM users WHERE username =" + "'" + username + "'", connection)
    stored_password = stored_password["hashed_password"].tolist()
    stored_password = stored_password[0]

    if bcrypt.checkpw(password.encode('utf-8'), stored_password):
        typer.echo("Password is correct")
    else:
        typer.echo("Password is incorrect")
        return
    
    response = user_status(username, "nexrad_s3_fetchurl")
    if response == "Too many requests wait for 1 hour":
        typer.echo(response)
        return
    
        
    year = typer.prompt("Enter year from 2022 to 2023", type = str)
    if year not in ['2022', '2023']:
        typer.echo("Invalid year")
        return

    # month_selected = None
    # day_selected = None
    # station_selected = None
    # file_selected = None
    
    FASTAPI_URL = "http://localhost:8081/nexrad_s3_fetch_month"
    response = requests.get(FASTAPI_URL, json={"yearSelected": year})

    if response.status_code == 200:
        month = response.json()
        month = month['Month']

        month_selected = typer.prompt("Enter month from \n", month)
        if month_selected not in month:
            typer.echo("Invalid month")
            return
        
    FASTAPI_URL = "http://localhost:8081/nexrad_s3_fetch_day"
    response = requests.get(FASTAPI_URL, json={"year": str(year), "month": str(month_selected)})

    if response.status_code == 200:
        day = response.json()
        day = day['Day']

        day_selected = typer.prompt("Enter day from \n", day)
        if day_selected not in day:
            typer.echo("Invalid day")
            return

    FASTAPI_URL = "http://localhost:8081/nexrad_s3_fetch_station"
    response = requests.get(FASTAPI_URL, json={"year": year, "month": month_selected, "day": day_selected})

    if response.status_code == 200:
        station = response.json()
        station = station['Station']

        station_selected = typer.prompt("Enter station from \n", station)
        if station_selected not in station:
            typer.echo("Invalid station")
            return
                
    FASTAPI_URL = "http://localhost:8081/nexrad_s3_fetch_file"
    response = requests.get(FASTAPI_URL, json={"year": year, "month": month_selected, "day": day_selected, "station": station_selected})

    if response.status_code == 200:
        file = response.json()
        file = file['File']

        file_selected = typer.prompt("Enter file from \n", file)
        if file_selected not in file:
            typer.echo("Invalid file")
            return

    FASTAPI_URL = "http://localhost:8081/nexrad_s3_fetchurl"
    response = requests.post(FASTAPI_URL, json={"year": year, "month": month_selected, "day": day_selected, "station": station_selected, "file": file_selected})

    if response.status_code == 200:
        url = response.json()
        typer.echo(url['Public S3 URL'])

    else:
        typer.echo("Invalid filename or inputs")
        return



@app.command()
def fetch(username: str, password:str, bucket_name:str):
               
    """
    List all files in an public S3 bucket

    Args:
        username (str): User name
        bucket_name (str): S3 bucket name
    
    Returns:
        None
    """

    s3client = create_connection()
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM Users", connection)
    user_lst = users["username"].tolist()

    if username not in user_lst:
        typer.echo(f"User {username} does not exist")
        return

    stored_password = pd.read_sql("SELECT hashed_password FROM users WHERE username =" + "'" + username + "'", connection)
    stored_password = stored_password["hashed_password"].tolist()
    stored_password = stored_password[0]

    if bcrypt.checkpw(password.encode('utf-8'), stored_password):
        typer.echo("Password is correct")
    else:
        typer.echo("Password is incorrect")
        return
    
    response = user_status(username, "s3_fetch_keys")
    if response == "Too many requests wait for 1 hour":

        typer.echo("Too many requests wait for 1 hour")
        return
    
    
    typer.confirm(f"Are you sure you want to list files in S3 bucket?", abort=True)
    typer.echo("Listing files in S3 bucket.........")

    FASTAPI_URL = "http://localhost:8000/s3_fetch_keys"
    response = requests.get(FASTAPI_URL, json={"bucket_name": bucket_name})

    if response.status_code == 200:
        keys = response.json()
        typer.echo(keys['Keys'])
    else:
        typer.echo("Invalid bucket name")
        return




@app.command()
def download(username: str, password:str, bucket_name: str = typer.Argument("damg7245-team7"), file_name: str = typer.Argument(...)):
    """
    Download a file from an S3 bucket

    Args:
        username (str): User name
        bucket_name (str): S3 bucket name
        file_name (str): File name  
    
    Returns:
        None
    """

    s3client = create_connection()
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM Users", connection)
    user_lst = users["username"].tolist()

    if username not in user_lst:
        typer.echo(f"User {username} does not exist")
        return

    stored_password = pd.read_sql("SELECT hashed_password FROM users WHERE username =" + "'" + username + "'", connection)
    stored_password = stored_password["hashed_password"].tolist()
    stored_password = stored_password[0]

    if bcrypt.checkpw(password.encode('utf-8'), stored_password):
        typer.echo("Password is correct")
    else:
        typer.echo("Password is incorrect")
        return
    
    response = user_status(username, "download_s3_file")
    if response == "Too many requests wait for 1 hour":
        typer.echo(response)
        return

    # Check if the file exists in the bucket

    FASTAPI_URL = "http://localhost:8000/download_s3_file"

    response = requests.get(FASTAPI_URL, json={"bucket_name": bucket_name, "file_name": file_name})

    if response.status_code == 200:
        typer.echo(f"Downloading file '{file_name}' from S3 bucket '{bucket_name}'...")
        s3client.download_file(bucket_name, file_name, file_name)
        typer.echo("File downloaded successfully")
    else:
        typer.echo("Invalid bucket name or file name")
        return




@app.command()
def fetchnexrad_filename (username: str, password: str):

    """
    Fetch Nexrad filename from the NOAA website

    Args:
        username (str): User name
        password (str): Password
    
    Returns:
        None
    """

    s3client = create_connection()
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM Users", connection)
    user_lst = users["username"].tolist()

    if username not in user_lst:
        typer.echo(f"User {username} does not exist")
        return

    stored_password = pd.read_sql("SELECT hashed_password FROM users WHERE username =" + "'" + username + "'", connection)
    stored_password = stored_password["hashed_password"].tolist()
    stored_password = stored_password[0]

    if bcrypt.checkpw(password.encode('utf-8'), stored_password):
        typer.echo("Password is correct")
    else:
        typer.echo("Password is incorrect")
        return

    response = user_status(username, "nexrad_get_download_link")
    if response == "Too many requests wait for 1 hour":
        typer.echo(response)
        return
    
    
    
    file_name = typer.prompt("Enter file name")
    FASTAPI_URL = "http://localhost:8081/nexrad_get_download_link"
    response = requests.post(FASTAPI_URL, json={"filename": file_name})

    if response.status_code == 200:
        url = response.json()
        typer.echo(url['Response'])

    else:
        typer.echo("File does not exist or invalid file name")



if __name__ == "__main__":
    app()