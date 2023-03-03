import streamlit as st
import pandas as pd
import numpy as np
import plost
import os
import requests
import sys
from dotenv import load_dotenv
from datetime import datetime,timedelta
# import matplotlib.pyplot as plt


# from PIL import Image

ACCESS_TOKEN = os.environ["access_token"]
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

# Page setting
st.set_page_config(layout="wide")

with open('pages/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

url="http://localhost:8000/get_current_username"
response=requests.post(url,headers=headers)
username=response.json()


st.title('Dashboard')
st.text('Welcome ' + str(username['username']) + '!')



def convert_to_date(date_string,format):
    
    date_object = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S.%f")
    
    if format=='date':
        return date_object.date().strftime('%Y-%m-%d')
    elif format=='hour':
        return date_object.hour
    elif format=='month':
        return date_object.month
    elif format=='week':
        return date_object.isocalendar()[1]

def user_act_data(timeframe):
    
    if timeframe=="Hour":
        last_date = datetime.strptime(user_data['date'].iloc[-1], '%Y-%m-%d %H:%M:%S.%f')
        time_diff = datetime.utcnow() - last_date
        if time_diff <= timedelta(hours=1):
            api_hits=user_data['hit_count'].iloc[-1]
            rem_limit=user_data['api_limit'].iloc[-1]-api_hits
        else:
            api_hits=0
            rem_limit=user_data['api_limit'].iloc[-1]
            
    return api_hits,rem_limit

def data_charts(timeframe):
    
    if timeframe=='Day':
        # print(user_data.columns)
        
        daily_count = user_data.groupby(['date_str','api_name'],as_index=False).agg({'date': 'count'})
        daily_count = daily_count.reset_index()
        daily_count = daily_count.rename(columns={'date': 'API Hits'})
        # print('y')
        return daily_count
    
    elif timeframe=='Week':
        week_count = user_data.groupby(['week','api_name']).agg({'date': 'count'})
        # reset index and rename columns
        week_count = week_count.reset_index()
        week_count = week_count.rename(columns={'date': 'API Hits'})
        return week_count
    
    elif timeframe=='Month':
        month_count = user_data.groupby(['month','api_name']).agg({'date': 'count'})
        # reset index and rename columns
        month_count = month_count.reset_index()
        month_count = month_count.rename(columns={'date': 'API Hits'})
        return month_count
    
def make_chart(data,x_axis,y_axis,type,title):
    if type=='area':
        st.markdown(title)
        st.area_chart(
        data=data,
        x=x_axis,
        y=y_axis)
    elif type=='bar':
        st.markdown(title)
        st.bar_chart(
        data=data,
        x=x_axis,
        y=y_axis)

try:
    fastapi_url="http://localhost:8000/get_useract_data"
    response=requests.get(fastapi_url,headers=headers)
except:
    print('user activity data not yet generated') 

try:
# print(response)
    
    if response.status_code==200:
        
        user_data_json=response.json()
        # print(user_data_json['data'])
        user_data = pd.DataFrame(user_data_json['data'])
        user_data['date_str']=user_data['date'].apply(convert_to_date,args=('date',))
        user_data['hour']=user_data['date'].apply(convert_to_date,args=('hour',))
        user_data['month']=user_data['date'].apply(convert_to_date,args=('month',)) 
        user_data['week']=user_data['date'].apply(convert_to_date,args=('week',)) 
        
    else:
        st.error("You haven't yet used our application")
        user_data=pd.DataFrame(columns=['username','service_plan','api_limit','date','api_name','hit_count','date_str','hour','month','week'])
    
    st.text('Your current plan - ' + str(user_data['service_plan'].iloc[-1]))


    api_hits=0
    rem_limit=0
    b1, b2 = st.columns(2)
    b1.metric("API HITs", user_act_data('Hour')[0])
    b2.metric("Remaining limit", user_act_data('Hour')[1])

    view_selection = st.radio("View by:",
            options=["Day", "Week", "Month"],
            horizontal=True
        )

    if view_selection=="Day":
        data=data_charts('Day')
        recent_date=data['date_str'].iloc[-1]
        recent_data=data[data['date_str']==recent_date]
        # print('000000')
        # print(recent_data)
        all_data = data.groupby(['date_str'],as_index=False).agg({'API Hits': 'sum'})
        all_data = all_data.reset_index()
        # all_data = all_data.rename(columns={'date': 'API Hits'})
        col1, col2 = st.columns(2)
        with col1:
            make_chart(recent_data,'api_name','API Hits','bar','### API Usage - Modules ')
        with col2:    
            make_chart(all_data,'date_str','API Hits','bar','### API Usage - Total')
    elif view_selection=="Week":
        data=data_charts('Week')
        recent_date=data['week'].iloc[-1]
        recent_data=data[data['week']==recent_date]
        make_chart(recent_data,'api_name','API Hits','bar','### API Usage - Modules ')
    elif view_selection=="Month":
        data=data_charts('Month')
        recent_date=data['month'].iloc[-1]
        recent_data=data[data['month']==recent_date]
        make_chart(recent_data,'api_name','API Hits','bar','### API Usage - Modules ')
        
except:
    print('empty table error')


# st.markdown('### API USAGE by modules')
# st.bar_chart(
# data=user_data,
# x='date_str',
# y='api_name'
# )

# fig, ax = plt.subplots()
# user_data.plot.bar(x='date_str', 
#                 y = 'api_name',
#                 stacked = True, ax=ax)
# st.pyplot(fig)