import streamlit as st
import requests
import os
from dotenv import load_dotenv
from PIL import Image
import webbrowser

load_dotenv()

no_sidebar_style = """
    <style> 
        div[data-testid="stSidebarNav"] {display: none;}
        </style>
        
        """

st.set_page_config(page_title="Signup", initial_sidebar_state="collapsed")
st.markdown(no_sidebar_style, unsafe_allow_html=True)
with st.sidebar:
    if st.button("Login"):
        webbrowser.open("http://localhost:8501/login")

with st.container():
        username = st.text_input('Enter Username')
        password = st.text_input('Enter Password', type='password')
        st.text("")
        st.write("Choose a plan that works for you")
        image = Image.open('plans.jpg')
        st.image(image)
        plan = st.radio(" ",('Free', 'Platinum', 'Gold'), horizontal=True)
        st.text("")
        if st.button('Signup'):
            if username == "" or password == "":
                st.error('Username or Password is empty')
            else:
                if plan == 'Free':
                     api_limit = 10
                elif plan == 'Gold':
                     api_limit = 15
                else:
                     api_limit = 20
                data = {
                "username": username,
                "password": password,
                "service_plan": plan,
                "api_limit": api_limit
                }

                FASTAPI_URL = "http://localhost:8000/signup"
                response = requests.post(FASTAPI_URL,json=data)
                if int(response.json()['status_code']) == 200:
                     st.success('User Registered Successfully', icon="✅")
                else:
                     st.error('Error Registering User! Please Try Again!')