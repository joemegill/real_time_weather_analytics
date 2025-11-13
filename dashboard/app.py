import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

st.title("🌤 Real-Time Weather Dashboard")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@postgres:5432/weatherdb")


@st.cache_data(ttl=60)
def get_data():
    return pd.read_sql(
        "SELECT * FROM hourly_weather_data ORDER BY timestamp ASC LIMIT 100", engine
    )


@st.cache_data(ttl=60)
def get_data_current():
    return pd.read_sql(
        "SELECT * FROM current_weather_data ORDER BY timestamp DESC LIMIT 10", engine
    )


@st.cache_data(ttl=60)
def get_data_minute():
    return pd.read_sql(
        "SELECT * FROM minute_weather_data ORDER BY timestamp DESC LIMIT 60", engine
    )


df = get_data()

df_cur = get_data_current()

df_min = get_data_minute()

st.dataframe(pd.read_sql("SELECT * FROM current_weather_data", engine))


fig = px.line(df, x="timestamp", y="temp", title="Hourly Temperature Over Time")
st.plotly_chart(fig)

st.dataframe(df)

st.title("🌤 Current Weather")
st.dataframe(df_cur)

st.title("Precipitation Over The Next 60 Minute")
fig2 = px.line(
    df_min,
    x="timestamp",
    y="precipitation",
    range_y=[0.0, 305.0],
    title="Hourly Temperature Over Time",
)
st.plotly_chart(fig2)
st.dataframe(df_min)
