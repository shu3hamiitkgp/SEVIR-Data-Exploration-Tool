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


from api_codes import nexrad_api
from backend import nexrad_main


database_path = os.path.join(project_dir, 'database.db')
app = typer.Typer()




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
def createuser(user_name: str):
    """ 
    Create a user in the system

    Args:
        user_name (str): User name

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
    
        df = pd.DataFrame({"user_name": [user_name], "password": [password], "tier": [user_tier]})
        df.to_sql("users", con=sqlite3.connect(database_path), if_exists="append", index=False)
        typer.echo(f"User {user_name} created successfully")


@app.command()
def fetchnexrad(user_name: str, password: str):
               
    """
    Fecth nexrad data from S3 bucket

    Args:
        user_name (str): User name
        password (str): Password

    Returns:
        None
    """

    s3client = create_connection()

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM users", connection)
    user_lst = users["user_name"].tolist()

    if user_name not in user_lst:
        typer.echo(f"User {user_name} does not exist")
        return

    stored_password = pd.read_sql("SELECT password FROM users WHERE user_name =" + "'" + user_name + "'", connection)
    stored_password = stored_password["password"].tolist()
    stored_password = stored_password[0]

    if bcrypt.checkpw(password.encode('utf-8'), stored_password):
        typer.echo("Password is correct")
    else:
        typer.echo("Password is incorrect")
        return

        
    year = typer.prompt("Enter year from 2022 to 2023", type = str)
    if year not in ['2022', '2023']:
        typer.echo("Invalid year")
        return
    


    FASTAPI_URL = "http://localhost:8000/nexrad_s3_fetch_month"
    response = requests.get(FASTAPI_URL, json={"yearSelected": year})

    if response.status_code == 200:
        month = response.json()
        month = month['Month']

        month_selected = typer.prompt("Enter month from \n", month)
        if month_selected not in month:
            typer.echo("Invalid month")
            return
        
    FASTAPI_URL = "http://localhost:8000/nexrad_s3_fetch_day"
    response = requests.get(FASTAPI_URL, json={"year": str(year), "month": str(month_selected)})

    if response.status_code == 200:
        day = response.json()
        day = day['Day']

        day_selected = typer.prompt("Enter day from \n", day)
        if day_selected not in day:
            typer.echo("Invalid day")
            return

    FASTAPI_URL = "http://localhost:8000/nexrad_s3_fetch_station"
    response = requests.get(FASTAPI_URL, json={"year": year, "month": month_selected, "day": day_selected})

    if response.status_code == 200:
        station = response.json()
        station = station['Station']

        station_selected = typer.prompt("Enter station from \n", station)
        if station_selected not in station:
            typer.echo("Invalid station")
            return
                
    FASTAPI_URL = "http://localhost:8000/nexrad_s3_fetch_file"
    response = requests.get(FASTAPI_URL, json={"year": year, "month": month_selected, "day": day_selected, "station": station_selected})

    if response.status_code == 200:
        file = response.json()
        file = file['File']

        file_selected = typer.prompt("Enter file from \n", file)
        if file_selected not in file:
            typer.echo("Invalid file")
            return

    FASTAPI_URL = "http://localhost:8000/nexrad_s3_fetchurl"
    response = requests.post(FASTAPI_URL, json={"year": year, "month": month_selected, "day": day_selected, "station": station_selected, "file": file_selected})

    if response.status_code == 200:
        url = response.json()
        typer.echo(url['Public S3 URL'])

    else:
        typer.echo("Invalid filename or inputs")
        return



@app.command()
def fetch(user_name: str, bucket_name:str):
               
    """
    List all files in an public S3 bucket

    Args:
        user_name (str): User name
        bucket_name (str): S3 bucket name
    
    Returns:
        None
    """

    s3client = create_connection()

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM users", connection)
    user_lst = users["user_name"].tolist()

    if user_name not in user_lst:
        typer.echo(f"User {user_name} does not exist")
        return
    

    typer.confirm(f"Are you sure you want to list files in S3 bucket?", abort=True)
    typer.echo("Listing files in S3 bucket.........")
    objects = s3client.list_objects_v2(Bucket=bucket_name)

    for obj in objects.get("Contents", []):
        typer.echo(obj.get("Key"))

@app.command()
def download(user_name: str, bucket_name: str = typer.Argument("damg7245-team7"), file_name: str = typer.Argument(...)):
    """
    Download a file from an S3 bucket

    Args:
        user_name (str): User name
        bucket_name (str): S3 bucket name
        file_name (str): File name  
    
    Returns:
        None
    """
    s3client = create_connection()

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM users", connection)
    user_lst = users["user_name"].tolist()

    if user_name not in user_lst:
        typer.echo(f"User {user_name} does not exist")
        return
    

    # Check if the file exists in the bucket
    objects = s3client.list_objects_v2(Bucket=bucket_name)
    if not any(obj.get("Key") == file_name for obj in objects.get("Contents", [])):
        typer.echo(f"File '{file_name}' does not exist in S3 bucket '{bucket_name}'.")
        return
    
    # Download the file
    typer.echo(f"Downloading file '{file_name}' from S3 bucket '{bucket_name}'...")
    s3client.download_file(bucket_name, file_name, file_name)

@app.command()
def fetchnexrad_filename (user_name: str, password: str):

    s3client = create_connection()

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    users = pd.read_sql_query("SELECT * FROM users", connection)
    user_lst = users["user_name"].tolist()

    if user_name not in user_lst:
        typer.echo(f"User {user_name} does not exist")
        return

    stored_password = pd.read_sql("SELECT password FROM users WHERE user_name =" + "'" + user_name + "'", connection)
    stored_password = stored_password["password"].tolist()
    stored_password = stored_password[0]

    if bcrypt.checkpw(password.encode('utf-8'), stored_password):
        typer.echo("Password is correct")
    else:
        typer.echo("Password is incorrect")
        return
    
    file_name = typer.prompt("Enter file name")
    FASTAPI_URL = "http://localhost:8000/nexrad_get_download_link"
    response = requests.post(FASTAPI_URL, json={"filename": file_name})

    if response.status_code == 200:
        url = response.json()
        typer.echo(url['Response'])

    else:
        typer.echo("File does not exist or invalid file name")



    




if __name__ == "__main__":
    app()