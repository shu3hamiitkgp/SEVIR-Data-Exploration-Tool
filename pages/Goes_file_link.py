import requests
import streamlit as st
import os
from dotenv import load_dotenv
import webbrowser

load_dotenv()

with st.sidebar:
    if st.button("Logout"):
        webbrowser.open("http://streamlit:8501/login")


ACCESS_TOKEN = os.environ["access_token"]
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

st.title("Generate NOAA-GOES18 URL BY FILE")

response = requests.get('http://localhost:8000/is_logged_in',headers=headers)


if response.status_code == 200:
    file_name = st.text_input('Enter File Name')
    st.text("")
    if st.button('Get URL'):
        with st.spinner('Processing...'):
            FASTAPI_URL='http://localhost:8000/user_api_status'
            input={'api_name':'goes_filename'}
            response=requests.post(FASTAPI_URL,json=input,headers=headers)
            
            if response.status_code==200:
                
                if file_name:
                    response = requests.post('http://localhost:8000/validatefileUrl',json={'file_name': file_name},headers=headers)
                    if response.status_code != 401:
                        validate_res = response.json()['message']
                        st.text("")
                        if validate_res == 'Valid filename':
                            response1 = requests.post('http://localhost:8000/getfileUrl',json={'file_name': file_name},headers=headers)
                            if response1.status_code!= 401:
                                get_res = response1.json()
                                if get_res['status_code'] == '404':
                                    st.error('File does not exist. Please check the file name and try again!', icon="🚨")
                                else:
                                    st.write('Download URL:  \n ', get_res['message'])
                            else:
                                st.error('Either you have not logged in or else your session has expired.', icon="🚨")
                        elif validate_res == 'Authentication Error':
                            st.write('You are not authorized to access this file.')
                        else:
                            st.warning(validate_res, icon="⚠️")
                    else:
                        st.error('Either you have not logged in or else your session has expired.', icon="🚨")
                else:
                    st.warning('Please enter a file name!', icon="⚠️")
else:
    st.error('Either you have not logged in or else your session has expired.', icon="🚨")