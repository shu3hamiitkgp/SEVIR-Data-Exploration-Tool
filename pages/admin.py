import streamlit as st
import requests
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import plost
import plotly.express as px
from datetime import datetime

load_dotenv()

no_sidebar_style = """
    <style> 
        div[data-testid="stSidebarNav"] {display: none;}
        </style>
        
        """
st.set_page_config(layout='wide', initial_sidebar_state='expanded')
st.markdown(no_sidebar_style, unsafe_allow_html=True)

getAnalyticsData = requests.get('http://localhost:8000/getAnalyticsData')
print(getAnalyticsData)
print(getAnalyticsData.json())
print(getAnalyticsData.json()['df_dict'])
df = pd.DataFrame(getAnalyticsData.json()['df_dict'])

getUsersData = requests.get('http://localhost:8000/getUsersData')
df_user = pd.DataFrame(getUsersData.json()['df_dict'])

print(df)

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    
st.sidebar.header('Dashboard')

st.sidebar.subheader('Time specific parameters')
metrics_duration = st.sidebar.selectbox('Metrics Duration', ('Per Day', 'Per Month', 'Per Year')) 

st.sidebar.subheader('User specific parameter')
user_list = ["All"]
for i in df_user['username']:
    if i!='admin' and i!=None:
        user_list.append(i)
selected_user = st.sidebar.selectbox('User', user_list)

def Dashboard(df_input,df_previous):
    total_users = len(df_input['username'].unique())
    total_users_prev = len(df_previous['username'].unique())
    if total_users_prev!=0:
        total_increase = ((total_users-total_users_prev)/total_users_prev)*100
    else:
        if total_users!=0:
            total_increase = 100
        else:
            total_increase = 0

    df_free = df_input[df_input['service_plan']=='free']
    total_free_users = len(df_free['username'].unique())
    # free_users_inc
    df_free_prev = df_previous[df_previous['service_plan']=='free']
    total_free_users_prev = len(df_free_prev['username'].unique())
    if total_free_users_prev!=0:
        free_increase = ((total_free_users-total_free_users_prev)/total_free_users_prev)*100
    else:
        if total_free_users!=0:
            free_increase = 100
        else:
            free_increase = 0

    df_gold = df_input[df_input['service_plan']=='gold']
    total_gold_users = len(df_gold['username'].unique())
    # gold_users_inc
    df_gold_prev = df_previous[df_previous['service_plan']=='gold']
    total_gold_users_prev = len(df_gold_prev['username'].unique())
    if total_gold_users_prev!=0:
        gold_increase = ((total_gold_users-total_gold_users_prev)/total_gold_users_prev)*100
    else:
        if total_gold_users!=0:
            gold_increase = 100
        else:
            gold_increase = 0

    df_platinum = df_input[df_input['service_plan']=='platinum']
    total_platinum_users = len(df_platinum['username'].unique())
    # free_users_inc
    df_platinum_prev = df_previous[df_previous['service_plan']=='platinum']
    total_platinum_users_prev = len(df_platinum_prev['username'].unique())
    if total_platinum_users_prev!=0:
        platinum_increase = ((total_platinum_users-total_platinum_users_prev)/total_platinum_users_prev)*100
    else:
        if total_platinum_users!=0:
            platinum_increase = 100
        else:
            platinum_increase = 0

    st.markdown('### User Metrics')
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Users", str(total_users), str(total_increase)+"%")
    col2.metric("Total Free Users", str(total_free_users), str(free_increase)+"%")
    col3.metric("Total Gold Users", str(total_gold_users), str(gold_increase)+"%")
    col4.metric("Total Platinum Users", str(total_platinum_users), str(platinum_increase)+"%")

    username = df_input['username'].value_counts().to_frame().to_dict()
    df_barGraph = pd.DataFrame(columns=['username', 'count'])
    for i in username['username']:
        df_barGraph = df_barGraph.append({'username':i, 'count':username['username'][i]}, ignore_index=True)

    c1, c2 = st.columns((7,3))
    with c1:
        st.write("## Histogram of api requests")
        fig = px.histogram(df_input, x='api_name', width=600, height=450)
        st.plotly_chart(fig)
    with c2:
        st.markdown('### Donut chart')
        plost.donut_chart(
            data=df_barGraph,
            theta='count',
            color='username',
            legend='bottom', 
            use_container_width=True)

df['date'] = pd.to_datetime(df['date'])
df['month'] = df['date'].dt.month
df['year'] = df['date'].dt.year
df['day'] = df['date'].dt.day

if metrics_duration == 'Per Day':
    current_day = datetime.now().day
    current_month = datetime.now().month
    current_year = datetime.now().year
    if len(df)>0:
        df_input = df[(df['day']==current_day) & (df['month']==current_month) & (df['year']==current_year)]
        df_previous = df[(df['day']==(current_day-1)) & (df['month']==current_month) & df['year']==current_year]
        Dashboard(df_input,df_previous)
    else:
        print("There is no data to show")

if metrics_duration == 'Per Month':
    current_month = datetime.now().month
    current_year = datetime.now().year
    if len(df)>0:
        df_input = df[(df['month']==current_month) & (df['year']==current_year)]
        df_previous = df[(df['month']==(current_month-1)) & (df['year']==current_year)]
        Dashboard(df_input,df_previous)
    else:
        print("There is no data to show")

if metrics_duration == 'Per Year':
    current_month = datetime.now().month
    current_year = datetime.now().year
    if len(df)>0:
        df_input = df[df['year']==current_year]
        df_previous = df[df['year']==(current_year-1)]
        Dashboard(df_input,df_previous)
    else:
        print("There is no data to show")