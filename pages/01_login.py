import streamlit as st
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import webbrowser

load_dotenv()



no_sidebar_style = """
    <style> 
        div[data-testid="stSidebarNav"] {display: none;}
        </style>
        
        """
block_sidebar_style = """
    <style> 
        div[data-testid="stSidebarNav"] {display: block;}
        </style>
        
        """

st.set_page_config(page_title="Login", initial_sidebar_state="collapsed")
st.markdown(no_sidebar_style, unsafe_allow_html=True)
with st.container():
        Username = st.text_input('Enter your username')
        Password = st.text_input('Enter your password', type='password')
        if st.button('Verify'):
            if Username == "" or Password == "":
                st.error('Username or Password value is empty')
            else:
                data = {
                "grant_type": "password",
                "username": Username,
                "password": Password
                }
                response = requests.post('http://localhost:8000/login',data=data)
                if int(response.json()['status_code']) == 200:
                    os.environ["access_token"] = response.json()["access_token"]
                    requests.post('http://localhost:8000/update_login',headers={"Authorization": f"Bearer {response.json()['access_token']}"})
                    # with open(".env", "a") as f:
                    #     f.write(f"access_token={response.json()['access_token']}\n")

                    with open(".env", "r") as f:
                        lines = f.readlines()

                    # Find the line that contains the access token
                    for i, line in enumerate(lines):
                        if line.startswith("access_token="):
                            # Replace the access token with the new value
                            lines[i] = "access_token=" + response.json()['access_token'] + "\n"
                            break

                    # Write the modified lines back to the file
                    with open(".env", "w") as f:
                        f.writelines(lines)
                    st.success('Login Successful')
                    if response.json()['service_plan']=='admin':
                        webbrowser.open("http://localhost:8501/admin")
                    # show sidebar
                    st.markdown(block_sidebar_style, unsafe_allow_html=True)


                elif int(response.json()['status_code']) == 404:
                    st.error('Username not found in the database')
                else:
                    st.error('Password is incorrect')

if st.button("Signup"):
     webbrowser.open("http://localhost:8501/User_Signup")