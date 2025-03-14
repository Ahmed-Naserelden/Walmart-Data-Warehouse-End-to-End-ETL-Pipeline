import os
import logging
import pandas as pd
import mysql.connector
import psycopg2

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    """Extract employee data from MySQL and save it to a CSV file."""
    conn = get_mysql_connection()
    if not conn:
        return

    cursor = conn.cursor()
    query = "SELECT * FROM Employee;"
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        employees_df = pd.DataFrame(data, columns=columns)
        employees_df.to_csv('/tmp/employees.csv', index=False)
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
        file_path = '/tmp/employees.csv'
        if not os.path.exists(file_path):
            logger.error("Extracted file not found.")
            return

        employees = pd.read_csv(file_path)
        if employees.empty:
            logger.error("Extracted CSV file is empty.")
            return

        if 'EmployeeName' not in employees.columns:
            logger.error("Column 'EmployeeName' not found in data.")
            return

        employees.rename(columns={'EmployeeName': 'employee_name'}, inplace=True)
        employees.to_csv('/tmp/employees_transformed.csv', index=False)
        logger.info("Data transformation completed successfully.")
    except Exception as e:
        logger.error(f"Error during transformation: {e}")

def load_data():
    """Load transformed data into the PostgreSQL database."""
    try:
        employees = pd.read_csv('/tmp/employees_transformed.csv')
        conn = get_postgres_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        query = """
            INSERT INTO dimemployee (employeebk, employeename, nationalid, jobtitle, gender, hiredate, salary)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        for _, row in employees.iterrows():
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
with DAG( dag_id="employees_etl_dag",
        default_args=default_args,
        schedule_interval="* * * * *",
        catchup=False,
        description="ETL DAG for extracting, transforming, and loading employee data."
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
