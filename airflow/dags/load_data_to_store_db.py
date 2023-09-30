import random

from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator


db_connection_data = dict(
    host='store-db',
    port='5432',
    user='postgres',
    password='password',
    database='store'
)

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 29)
}

def insert_store_data_to_db():
    
    users_query = ('INSERT INTO Users (userId, age) '
                   'VALUES (%s, %s)')
    
    items_query = ('INSERT INTO Items (ItemId, price) '
                   'VALUES (%s, %s)')
    
    purchases_query = ('INSERT INTO Purchases (userId, itemId, date) '
                       'VALUES (%s, %s, %s)')
    
    users_data = [(user_id, random.randint(18, 60)) for user_id in range(1, 101)]
    items_data = [(item_id, random.randint(200, 5000) + 0.99) for item_id in range(1, 8)]
    purchases_data = [
        (random.randint(1, 100), 
         random.randint(1, 7), 
         datetime(year=2023, 
                  month=random.randint(8, 9), 
                  day=random.randint(1, 29), 
                  hour=random.randint(9, 23),
                  minute=random.randint(1, 59)
                  ),
         )
         for i in range(1000)
         ]
    purchases_data.sort(key=lambda data: data[2])
    
    connection = psycopg2.connect(**db_connection_data)
    cursor = connection.cursor()
    cursor.executemany(users_query, users_data)
    cursor.executemany(items_query, items_data)
    cursor.executemany(purchases_query, purchases_data)

    connection.commit()
    cursor.close()
    connection.close()
    

with DAG(
    'init_store_data',
    catchup=False,
    schedule='@once',
    default_args=args
):
    task1 = BashOperator(
        task_id='echo',
        bash_command='echo {text}'.format(text='"Hello! I am starting to load data into the database."')
    )

    task2 = PythonOperator(
        task_id='load_store_data',
        python_callable=insert_store_data_to_db
    )

    task1 >> task2
