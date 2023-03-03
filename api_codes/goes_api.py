import ast
import json
import sqlite3
import os
from fastapi import Depends, FastAPI, Response, status
import requests
import sys
os.chdir('..')
sys.path.append('../Assignment_02')
from backend import main_goes18, goes_file_retrieval_main, oauth2
from pydantic import BaseModel
import pandas as pd
import re
from fastapi import APIRouter
from backend import schema
from backend.nexrad_main import write_logs




goes_database_file_name = 'goes18.db'
goes_database_file_path = os.path.join('data/',goes_database_file_name)


# app = FastAPI()

router = APIRouter()


@router.get('/s3_fetch_db')
async def s3_fetch_db():

    """Fetches the database from the S3 bucket
    
    Args:
        None
        
    Returns:
        None"""

    main_goes18.fetch_db()

    return Response(status_code=status.HTTP_200_OK)

@router.get('/goes_station')
async def grab_station(getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    """for pulling all the stations in the file from database

    Returns:
        stations_list: list of stations
    """
    stations=main_goes18.grab_station()
    
    return {'Stations':stations}


@router.get('/goes_years')
async def grab_years(user_station: schema.goes_year, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user) ):
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

@router.get('/goes_days')
async def grab_months(user_day: schema.goes_day, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
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

@router.get('/goes_hours')
async def grab_hours(user_hour: schema.goes_hour, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    
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

@router.get('/goes_files')
async def grab_files(user_files: schema.goes_file, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
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


@router.post('/goes_fetch_url')
async def create_url(user_url: schema.goes_url, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    
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
        
@router.post('/goes_AWS_url')
async def s3_url(user_purl: schema.goes_url, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
     
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


@router.post("/validatefileUrl")
def validate_file(validateFile: schema.ValidateFile,token_data: schema.TokenData = Depends(oauth2.get_current_user)):
  filename = validateFile.file_name
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

  print("000000")
  if(len(file_name_split)>6):
    write_logs("File name is invalid")
    write_logs("File validation function execution complete")
    data = {'message': 'File name is invalid','status_code': '200'}
    print("1111111111")
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
  print("333333333")
  data = {'message': 'Valid filename', 'status_code': '200'}
  print(data)
  return data



@router.post("/getfileUrl")
async def getFileUrl(validateFile: schema.ValidateFile, getCurrentUser: schema.TokenData = Depends(oauth2.get_current_user)):
    filename = validateFile.file_name
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