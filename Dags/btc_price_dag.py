from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pymysql

def fetch_btc_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice/BTC.json"
    response = requests.get(url)
    data = response.json()
    price_usd = float(data["bpi"]["USD"]["rate"].replace(",", ""))
    timestamp = datetime.utcnow()

    connection = pymysql.connect(
        host="mysql-airflow",
        user="root",
        password="rohith5799",
        database="btc"
    )
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO btc_price (price_usd, timestamp) VALUES (%s, %s)",
        (price_usd, timestamp)
    )
    connection.commit()
    cursor.close()
    connection.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "btc_price_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

task = PythonOperator(
    task_id="fetch_btc_price",
    python_callable=fetch_btc_price,
    dag=dag,
)
