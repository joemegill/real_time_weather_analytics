import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

DB_USER=os.getenv("DB_USER")
DB_PASS=os.getenv("DB_PASS")
DB_HOST=os.getenv("DB_HOST")
DB_NAME=os.getenv("DB_NAME")

st.title("🌤 Real-Time Weather Dashboard")

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@postgres:5432/weatherdb')


@st.cache_data(ttl=60)
def get_data():
    return pd.read_sql("SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 100", engine)

df = get_data()

fig = px.line(df, x='timestamp', y='temp', title='Temperature Over Time')
st.plotly_chart(fig)

st.dataframe(df)
