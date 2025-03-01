import os
import logging
import pandas as pd
import mysql.connector
import psycopg2
import numpy as np

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_surrogate_key(cursor, df_bk, col_id, dim_table, business_column, surrogate_column):
    query = f"SELECT {business_column}, {surrogate_column} FROM {dim_table}"
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=[business_column, surrogate_column])
    key_map = dict(zip(df[business_column], df[surrogate_column]))
    return df_bk[col_id].map(key_map)

# Default arguments for DAG
default_args = {
    'owner': 'biruni',
    'retries': 1,
    'start_date': datetime(2025, 1, 19),
    'email_on_failure': True,
    'email': ["an2071497@gmail.com", "ahmednsereldin@gmail.com"],
}

def get_mysql_connection():
    """Establish a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USERNAME'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        logger.info("Successfully connected to MySQL.")
        return conn
    except mysql.connector.Error as e:
        logger.error(f"MySQL connection error: {e}")
        return None

def get_postgres_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('PG_DATABASE'),
            user=os.getenv('PG_USERNAME'),
            password=os.getenv('PG_PASSWORD'),
            host=os.getenv('PG_HOST'),
            port=5432
        )
        logger.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logger.error(f"PostgreSQL connection error: {e}")
        return None

def extract_data():
    conn = get_mysql_connection()
    if not conn:
        return

    cursor = conn.cursor()

    query="""SELECT t.TransactionID, t.CustomerID, t.EmployeeID, td.ProductID, t.StoreID, t.PromotionID, t.TransactionDate, td.Quantity, t.TotalPrice 
    FROM Transaction t
    JOIN TransactionDetail td ON t.TransactionID = td.TransactionID;
    """
    
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = pd.DataFrame(data, columns=columns)
        data.to_csv('/tmp/transactions1.csv', index=False)
        logger.info("Data extraction completed successfully.")
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("MySQL connection closed.")

def transform_data():
    """Transform extracted data by renaming columns and handling errors."""
    try:
        file_path = '/tmp/transactions1.csv'
        if not os.path.exists(file_path):
            logger.error("Extracted file not found.")
            return

        data = pd.read_csv(file_path)
        if data.empty:
            logger.error("Extracted CSV file is empty.")
            return
        
        # transform TransactionDate to YYYYMMDD format
        data['TransactionDate'] = pd.to_datetime(data['TransactionDate'])
        data['TransactionDate'] = data['TransactionDate'].dt.strftime('%Y%m%d')

        data.to_csv('/tmp/transactions_transformed.csv', index=False)
        logger.info("Data transformation completed successfully.")
    except Exception as e:
        logger.error(f"Error during transformation: {e}")

def load_data():
    """Load transformed data into the PostgreSQL database."""
    try:
        conn = get_postgres_connection()
        if not conn:
            return
        
        cursor = conn.cursor()

        data = pd.read_csv('/tmp/transactions_transformed.csv')

        data['CustomerID'] = get_surrogate_key(cursor, data, 'CustomerID', 'dimcustomer', 'customerbk', 'customersk')
        data['EmployeeID'] = get_surrogate_key(cursor, data, 'EmployeeID', 'dimemployee', 'Employeebk', 'employeesk')  
        data['ProductID'] = get_surrogate_key(cursor, data, 'ProductID', 'dimproduct', 'Productbk', 'productSK')
        data['StoreID'] = get_surrogate_key(cursor, data, 'StoreID', 'dimstore', 'storebk', 'storesk')
        data['PromotionID'] = get_surrogate_key(cursor, data, 'PromotionID', 'dimpromotion', 'Promotionbk', 'promotionsk')
        
        data.to_csv('/tmp/transactions_transformed.csv', index=False)

        logger.info("transformed Data completed successfully.")

        query = """
            INSERT INTO factsales (transactionid, customersk, employeesk, productsk, storesk, promotionsk, datesk, quantity, totalprice)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    
        
        data.replace({np.nan: None}, inplace=True)
        
        for _, row in data.iterrows():
            cursor.execute(query, tuple(row))

        conn.commit()
        logger.info("Data loaded into PostgreSQL successfully.")
    
    except Exception as e:
        
        if conn:
            conn.rollback()
        logger.error(f"Error during data load: {e}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("PostgreSQL connection closed.")

# Define DAG
with DAG( dag_id="transactions_etl_dag",
        default_args=default_args,
        schedule_interval="* * * * *",
        catchup=False,
        description="ETL DAG for extracting, transforming, and loading transactions data."
    ) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
