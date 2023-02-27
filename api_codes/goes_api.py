import ast
import json
import sqlite3
import os
from fastapi import FastAPI, Response, status
import requests
import sys
os.chdir('..')
print(os.getcwd())
sys.path.append('../Assignment_02')
print(sys.path)
from backend import main_goes18, goes_file_retrieval_main
from pydantic import BaseModel
import pandas as pd
import re

goes_database_file_name = 'goes18.db'
goes_database_file_path = os.path.join('data/',goes_database_file_name)


app = FastAPI()

class goes_year(BaseModel):
    
    station: str
    
class goes_day(BaseModel):
    station: str
    year: str
    
class goes_hour(BaseModel):
    station: str
    year: str
    day: str     
    
class goes_file(BaseModel):
    station: str
    year: str
    day: str
    hour: str

class goes_url(BaseModel):
    station: str
    year: str
    day: str
    hour: str
    file: str

@app.get('/s3_fetch_db')
async def s3_fetch_db():

    """Fetches the database from the S3 bucket
    
    Args:
        None
        
    Returns:
        None"""

    main_goes18.fetch_db()

    return Response(status_code=status.HTTP_200_OK)

@app.get('/goes_station')
async def grab_station():
    """for pulling all the stations in the file from database

    Returns:
        stations_list: list of stations
    """
    stations=main_goes18.grab_station()
    
    return {'Stations':stations}


@app.get('/goes_years')
async def grab_years(user_station: goes_year ):
    """for pulling all the years in the station from database

    Args:
        station (string): station name

    Returns:
        year_list: list of all the years for a particular station
    """
    # 
    
    if not re.match(r"[A-Za-z0-9\.,;:!?()\"'%\-]+",user_station.station):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    else:
        
        year_list=main_goes18.grab_years(user_station.station)    
        
        return {'Year':year_list}

@app.get('/goes_days')
async def grab_months(user_day: goes_day):
    """for pulling all the days in the particular station,year from database

    Args:
        station (str): station
        years (str): year

    Returns:
        day_list: list of days for a particular station,year
    """
    
    if not re.match(r"[A-Za-z0-9\.,;:!?()\"'%\-]+",user_day.station):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_day.year)<2022 or int(user_day.year)>2023:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    else:
        
        day_list=main_goes18.grab_days(user_day.station,user_day.year)
        return {'Day':day_list}

@app.get('/goes_hours')
async def grab_hours(user_hour: goes_hour):
    
    """for pulling all the hours in the file for a particular station,year,day

    Args:
        station (str): station name
        years (str): year
        days (str): day

    Returns:
        hour_list: list of all hours in the file for a particular station,year,day
    """
    
    if not re.match(r"[A-Za-z0-9\.,;:!?()\"'%\-]+",user_hour.station):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_hour.year)<2022 or int(user_hour.year)>2023:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_hour.day)<1 or int(user_hour.day)>365:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    else:
        hour_list=main_goes18.grab_hours(user_hour.station,user_hour.year,user_hour.day)
        return {'Hour':hour_list}

@app.get('/goes_files')
async def grab_files(user_files: goes_file):
    """pulls files from noaa18 aws bucket for a set of station, year,day,hour

    Args:
        station (str): station name
        years (str): year
        days (str): day
        hours (str): hour

    Returns:
        file_names: list of files present in noaa18 aws bucket for a set of station, year,day,hour
    """
    
    # client_id=create_connection()
    
    # write_logs("fetching Files in list from NOAA bucket")
    if not re.match(r"[A-Za-z0-9\.,;:!?()\"'%\-]+",user_files.station):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_files.year)<2022 or int(user_files.year)>2023:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_files.day)<1 or int(user_files.day)>365:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_files.hour)<0 or int(user_files.hour)>24:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
        
    else:
        files_list=main_goes18.grab_files(user_files.station,user_files.year,user_files.day,user_files.hour)
        return {"Files":files_list}


@app.post('/goes_fetch_url')
async def create_url(user_url: goes_url):
    
    if not re.match(r"[A-Za-z0-9\.,;:!?()\"'%\-]+",user_url.station):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_url.year)<2022 or int(user_url.year)>2023:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_url.day)<1 or int(user_url.day)>365:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_url.hour)<0 or int(user_url.hour)>24:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if not goes_file_retrieval_main.validate_file(user_url.file) == 'Valid filename':
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    else:
        
        url=main_goes18.create_url(user_url.station,user_url.year,user_url.day,user_url.hour,user_url.file)
        
        response = requests.get(url)
        
        if response.status_code == 200:
            return {'NOAAURL': url}
        else:
            return Response(status_code=status.HTTP_404_NOT_FOUND)
        
@app.post('/goes_AWS_url')
async def s3_url(user_purl: goes_url):
     
    if not re.match(r"[A-Za-z0-9\.,;:!?()\"'%\-]+",user_purl.station):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_purl.year)<2022 or int(user_purl.year)>2023:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_purl.day)<1 or int(user_purl.day)>365:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if int(user_purl.hour)<0 or int(user_purl.hour)>24:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    if not goes_file_retrieval_main.validate_file(user_purl.file) == 'Valid filename':
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    
    else:
        
        key = main_goes18.generate_key(user_purl.station,user_purl.year,user_purl.day,user_purl.hour,user_purl.file)
        url=main_goes18.copy_files_s3(key,user_purl.file)
        
        response = requests.get(url)
        
        if response.status_code == 200:
            return {'S3URL': url}
        else:
            return Response(status_code=status.HTTP_404_NOT_FOUND)
