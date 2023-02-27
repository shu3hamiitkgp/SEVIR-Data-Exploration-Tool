import pandas as pd
import plotly.express as px
import sqlite3
import os
import requests
import streamlit as st
import os
from dotenv import load_dotenv
import webbrowser

load_dotenv()

with st.sidebar:
    if st.button("Logout"):
        webbrowser.open("http://localhost:8501/login")


ACCESS_TOKEN = os.environ["access_token"]
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

st.title("NEXRAD Station Locations")
response = requests.post('http://localhost:8000/retrieve_plot_data',headers=headers)

if response.status_code == 200:
    df = pd.DataFrame(response.json()['df_dict'])
    #sql_query = pd.read_sql('SELECT * FROM NEXRAD_PLOT', con)
    #df = pd.DataFrame(sql_query, columns = ['Id', 'State', 'City','ICAO_Location_Identifier','Coordinates','Lat','Lon'])
    fig = px.scatter_geo(df,lat='Lat',lon='Lon')
    fig.update_layout(title = 'Nexrad Locations', title_x=0.5)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.error('Either you have not logged in or else your session has expired.', icon="🚨")
