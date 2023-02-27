import streamlit as st
from PIL import Image
import pandas as pd

from passlib.context import CryptContext
from pages import Nexrad
import sqlite3
import os
import requests
from dotenv import load_dotenv
import webbrowser

load_dotenv()

no_sidebar_style = """
    <style> 
        div[data-testid="stSidebarNav"] {display: none;}
        </style>
        
        """




st.markdown(no_sidebar_style, unsafe_allow_html=True)

def create_plot_table():
    response = requests.post('http://localhost:8000/create_plot_table')
    return response

def create_default_user():
    response = requests.post('http://localhost:8000/create_default_user')
    return response

if __name__ == "__main__":



    create_default_user()
    # create_plot_table()

    st.title("Are you ready to fetch some data?")
    st.text("Team07-Assignment 2")
    st.text("Let us fetch that for you!")
    image = Image.open('image.png')
    st.image(image, caption='four humans working to fetch satelite data')

    if st.button('Fetch Data'):
        webbrowser.open("http://localhost:8501/login")

    
