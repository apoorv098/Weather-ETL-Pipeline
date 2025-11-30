# ⛅ Automating Weather Data Pipelines with Airflow, Snowflake & Streamlit

## 📌 Project Overview
This project is an end-to-end data engineering pipeline that extracts real-time weather data for dynamic cities, loads it into a Data Lake (AWS S3), transforms it in a Data Warehouse (Snowflake), and visualizes it via a Streamlit dashboard.

## 🏗 Architecture
**OpenWeatherMap API** $\rightarrow$ **Python (Extract)** $\rightarrow$ **AWS S3 (Load)** $\rightarrow$ **Snowflake (Transform)** $\rightarrow$ **Streamlit (Visualize)**

*   **Orchestration:** Apache Airflow (Dockerized)
*   **Infrastructure:** AWS S3, Snowflake
*   **Language:** Python 3.9
*   **Visualization:** Streamlit

## 🚀 Features
*   **Dockerized Airflow:** Custom Docker image with verified dependencies.
*   **Secure Secrets Management:** Utilized Airflow Variables/Connections and Streamlit Secrets to protect credentials.
*   **Dynamic Pipeline:** DAG supports runtime configuration (e.g., `{"city": "Paris"}`) via Airflow Params.
*   **Cost Optimization:** Implemented manual triggers and data caching to minimize API and Compute costs.

## 🛠️ Setup & Run

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-username/weather-data-pipeline.git
    ```
2.  **Add Secrets**:
    *   Create `.streamlit/secrets.toml` for Snowflake credentials.
    *   Add AWS keys and OpenWeather API Key to Airflow Admin -> Connections/Variables.
3.  **Run Airflow**:
    ```bash
    docker-compose up --build
    ```
4.  **Run Dashboard**:
    ```bash
    streamlit run dashboard.py
    ```
