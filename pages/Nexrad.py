import requests
import streamlit as st
import json
from backend import nexrad_main, nexrad_main_sqlite
from pages import Nexrad
import os
import sqlite3
import warnings
import ast
import os
from dotenv import load_dotenv
import webbrowser


load_dotenv()

ACCESS_TOKEN = os.environ["access_token"]
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

import pandas as pd
warnings.filterwarnings("ignore")


data_path = 'data/'
database_file_name = 'assignment_01.db'
database_path = os.path.join('data/', database_file_name)


data_files = os.listdir('data/')

# current_dir = os.getcwd()

# # get the absolute path of the "assignment 2" folder
# db_path  = os.path.abspath(os.path.join(current_dir, "..", "Assignment_02"))

if 'database.db' not in os.listdir(os.getcwd()):
    FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetch_db"
    response = requests.get(FASTAPI_URL, headers=headers)
    if response.status_code == 200:
        st.success("Successfully connected to the database")
    else:
        st.error("Failed to connect to the database")

with st.sidebar:
    if st.button("Logout"):
        os.environ["access_token"] = ""
        webbrowser.open("http://localhost:8501/login")



st.title("Generate Link Nexrad")


response = requests.get('http://35.229.73.233:8000/is_logged_in',headers=headers)

if response.status_code == 200:

    # User selects the year
    yearSelected = st.selectbox(
        'Select the year',
        (None, '2022', '2023'), key = 'year')



    # User selects the month
    if yearSelected != None:
        FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetch_month"
        response = requests.get(FASTAPI_URL, json={"yearSelected": yearSelected},headers=headers)
        monthSelected = None
        if response.status_code == 200:
            month = response.json()
            month = month['Month']
            monthSelected = st.selectbox(
                'Select the month',
                tuple(month), key = 'month')
        else:
            st.error('Either you have not logged in or else your session has expired.', icon="🚨")
        

        # User selects the day
        if monthSelected != None:
            FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetch_day"
            response = requests.get(FASTAPI_URL, json={"year": yearSelected, "month": monthSelected},headers=headers)
            daySelected = None
            if response.status_code == 200:
                day = response.json()
                day = day['Day']
                daySelected = st.selectbox(
                    'Select the day',
                    tuple(day), key = 'day')
            else:
                st.error('Either you have not logged in or else your session has expired.', icon="🚨")

            # User selects the station
            if daySelected != None:
                FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetch_station"
                response = requests.get(FASTAPI_URL, json={"year": yearSelected, "month": monthSelected, "day": daySelected},headers=headers)
                stationSelected = None
                if response.status_code == 200:
                    station = response.json()
                    station = station['Station']
                    stationSelected = st.selectbox(
                        'Select the station',
                        tuple(station), key = 'station')
                else:
                    st.error('Either you have not logged in or else your session has expired.', icon="🚨")

            
            # User selects the file
                with st.spinner('Fetching Files...'):
                    if stationSelected != None:
                        FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetch_file"
                        response = requests.get(FASTAPI_URL, json={"year": yearSelected, "month": monthSelected, "day": daySelected, "station": stationSelected},headers=headers)
                        fileSelected = None
                        if response.status_code == 200:
                            file = response.json()
                            file = file['File']
                            fileSelected = st.selectbox(
                                'Select the file',
                                tuple(file), key = 'file')
                        else:
                            st.error('Either you have not logged in or else your session has expired.', icon="🚨")


                if st.button("Submit"):
                    with st.spinner('Generating Public S3 Link...'):
                        FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetchurl"

                        response = requests.post(FASTAPI_URL, json={"year": yearSelected, "month": monthSelected, "day": daySelected, "station": stationSelected, "file": fileSelected},headers=headers)
                        if response.status_code == 200:
                            generated_url = response.json()
                            st.success("Successfully generated Public S3 link")
                            st.markdown("**Public URL**")

                            st.write(generated_url['Public S3 URL'])
                        elif response.status_code == 401:
                            st.error('Either you have not logged in or else your session has expired.', icon="🚨")
                        else:
                            st.error("Error in generating Public S3 link")
                            st.write(response.json())

                    with st.spinner('Generating Custom Link...'):

                        monthSelected = str(monthSelected)
                        daySelected = str(daySelected)
                        stationSelected = str(stationSelected)

                        if len(monthSelected) == 1:
                            monthSelected = '0' + monthSelected
                        if len(daySelected) == 1:
                            daySelected = '0' + daySelected

                        
                        FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_fetch_key"
                        response = requests.get(FASTAPI_URL, json={"year": yearSelected, "month": monthSelected, "day": daySelected, "station": stationSelected, "file": fileSelected},headers=headers)
                        if response.status_code == 200:
                            obj_key = response.json()['Key']

                            FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_upload"
                            response = requests.post(FASTAPI_URL, json={"key": obj_key, "source_bucket": 'noaa-nexrad-level2', "target_bucket": 'damg7245-team7'},headers=headers)
                            if response.status_code == 200:
                                user_key = response.json()['Uploaded_Key']
                                FASTAPI_URL = "http://35.229.73.233:8000/nexrad_s3_generate_user_link"

                                response = requests.post(FASTAPI_URL, json={"target_bucket": 'damg7245-team7', "user_key": user_key},headers=headers)
                                if response.status_code == 200:
                                    user_url = response.json()['User S3 URL']
                                    st.success("Successfully uploaded to User S3 Bucket")
                                    st.markdown("**AWS S3 URL**")
                                    st.write(user_url)
                                else:
                                    st.error('Either you have not logged in or else your session has expired.', icon="🚨")
                            else:
                                st.error('Either you have not logged in or else your session has expired.', icon="🚨")
                        else:
                            st.error('Either you have not logged in or else your session has expired.', icon="🚨")
else:
    st.error('Either you have not logged in or else your session has expired.', icon="🚨")