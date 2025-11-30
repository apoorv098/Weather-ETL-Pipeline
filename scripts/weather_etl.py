import requests
import pandas as pd
import json
from datetime import datetime
import s3fs
import random

def run_weather_etl(api_key, access_key, secret_key, bucket_name, city):

    
    

    # 2. Extract Data from API
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Error fetching data! Status Code: {response.status_code}")
        print(f"Error Message: {response.text}")
        return



    data = response.json()


    # 3. Transform Data (Flatten JSON to a simple Dictionary)
    weather_data = {
        "city": data["name"],
        "temperature": data["main"]["temp"] - 273.15, # Convert Kelvin to Celsius
        "humidity": data["main"]["humidity"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"],
        "timestamp": datetime.now()
    }

    # 4. Load into DataFrame
    df_data = pd.DataFrame([weather_data])

    # 5. Save to S3 (CSV format)
    # File name includes timestamp to ensure uniqueness
    file_name = f"weather_data_{city}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    s3_path = f"s3://{bucket_name}/{file_name}"
    
    try:
        df_data.to_csv(s3_path, index=False, storage_options={
            "key": access_key,
            "secret": secret_key
        })
        print(f"Data successfully uploaded to {s3_path}")
    except Exception as e:
        print(f"AWS S3 Upload Error: {e}")

if __name__ == "__main__":
    run_weather_etl()