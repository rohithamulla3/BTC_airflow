# Bitcoin Price Data Pipeline

This project implements a real-time data pipeline using Apache Airflow, designed to fetch Bitcoin price data every 10 minutes and store it in a local MySQL database for downstream analytics.

## Key Features

- 📡 **API Integration**: Coindesk API used to fetch latest Bitcoin price.
- ⏱ **Scheduled Execution**: Airflow DAG runs every 10 minutes.
- 🛢 **MySQL Storage**: Data stored in MySQL table with timestamps.
- 🐍 **Python & Jupyter**: Development done in Python and tested using Jupyter.
- 🐳 **Dockerized Environment**: Uses Docker Compose for Airflow setup.
- ✅ **Modular Files**: Contains all source code, SQL, DAG, and setup files.
