import streamlit as st
import pandas as pd
import snowflake.connector

st.set_page_config(
    page_title="Weather Data Dashboard",
    page_icon="⛅",
    layout="wide"
)

# CONNECT TO SNOWFLAKE (SECURELY)
# We use @st.cache_resource so we don't reconnect every time you click a button
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

conn = init_connection()

# ----------------------------------------------------------------
# FETCH DATA
# ----------------------------------------------------------------
@st.cache_data(ttl=600) # Cache data for 10 mins to save Snowflake credits
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()

st.title("⛅ Live Weather Analytics Pipeline")
st.markdown("### Data flowing from OpenWeather API -> AWS S3 -> Snowflake")

# Query the Data
try:
    # Get the latest 50 records
    query = """
    SELECT * 
    FROM WEATHER_DATA 
    ORDER BY TIMESTAMP DESC 
    LIMIT 50
    """
    df = run_query(query)

    # ----------------------------------------------------------------
    # KEY METRICS (Top Row)
    # ----------------------------------------------------------------
    if not df.empty:
        latest_data = df.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Latest City", latest_data['CITY'])
        col2.metric("Temperature", f"{latest_data['TEMPERATURE']:.1f} °C")
        col3.metric("Humidity", f"{latest_data['HUMIDITY']}%")
        col4.metric("Wind Speed", f"{latest_data['WIND_SPEED']} m/s")

        # ----------------------------------------------------------------
        # CHARTS & TABLES
        # ----------------------------------------------------------------
        st.subheader("Temperature Trend (Last 50 Entries)")
        
        # Simple line chart for Temperature
        chart_data = df[['TIMESTAMP', 'TEMPERATURE']].set_index('TIMESTAMP')
        st.line_chart(chart_data)

        st.subheader("Raw Data from Snowflake")
        st.dataframe(df)
        
    else:
        st.warning("No data found in Snowflake yet. Run your Airflow DAG!")

except Exception as e:
    st.error(f"Error connecting to Snowflake: {e}")