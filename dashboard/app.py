import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.title("🌤 Real-Time Weather Dashboard")

engine = create_engine('postgresql://weather_user:weather_pass@postgres:5432/weatherdb')


@st.cache_data(ttl=60)
def get_data():
    return pd.read_sql("SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 100", engine)

df = get_data()

fig = px.line(df, x='timestamp', y='temperature', color='city', title='Temperature Over Time')
st.plotly_chart(fig)

st.dataframe(df)
