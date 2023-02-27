import streamlit as st
import requests
import os
from dotenv import load_dotenv

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
                    os.environ["access_token"] = response.json()['access_token']
                    # with open(".env", "a") as f:
                    #     f.write(f"access_token={response.json()['access_token']}\n")

                    with open(".env", "r+") as f:
                        contents = f.read()
                        # Get the position of the current access token in the file
                        token_position = contents.find("access_token")

                        # Move the file pointer to the beginning of the access token
                        f.seek(token_position)

                        # Overwrite the existing access token with the new one
                        f.write("access_token=" + response.json()['access_token'])

                        # Move the file pointer to the end of the file
                        f.seek(0, 2)

                        # Truncate any remaining contents after the new token
                        f.truncate()
                    st.success('Login Successful')
                    # show sidebar
                    st.markdown(block_sidebar_style, unsafe_allow_html=True)



                    


                elif int(response.json()['status_code']) == 404:
                    st.error('Username not found in the database')
                else:
                    st.error('Password is incorrect')