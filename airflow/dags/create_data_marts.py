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
    'start_date': datetime(2023, 9, 30)
}

def select_data_from_db(cursor):
    
    first_query = """SELECT between_18_and_25.prices_avg as between_18_and_25_prices_avg,
                            between_26_and_35.prices_avg as between_26_and_35_prices_avg,
                            purchases_35_plus.purchased_month as month_that_had_max_revenue,
                            total_purchases_by_items.itemId as item_id_that_generates_the_maximum_income_for_the_year
                     FROM (
                            SELECT round(avg(i.price), 2) as prices_avg
                            FROM purchases p
                            JOIN users u ON p.userid = u.userid
                            JOIN items i ON p.itemid = i.itemid
                            WHERE u.age >= 18 and u.age <= 25
                            ) as between_18_and_25,
                            (
                            SELECT round(avg(i.price), 2) as prices_avg
                            FROM purchases p
                            JOIN users u ON p.userid = u.userid
                            JOIN items i ON p.itemid = i.itemid
                            WHERE u.age >= 26 and u.age <= 35
                            ) as between_26_and_35,
                            (
                            SELECT to_char(p."date", 'MM') as purchased_month, sum(i.price) as total_sum_for_month_for_users_35_plus
                            FROM purchases p
                            JOIN users u ON p.userid = u.userid
                            JOIN items i ON p.itemid = i.itemid
                            WHERE u.age >= 35
                            GROUP BY purchased_month
                            ) as purchases_35_plus,
                            (
                            SELECT to_char(p."date", 'YYYY') as purchase_year, i.itemid, sum(i.price) as total
                            FROM purchases p
                            JOIN users u ON p.userid = u.userid
                            JOIN items i ON p.itemid = i.itemid
                            GROUP BY i.itemid, purchase_year
                            ) as total_purchases_by_items
                     WHERE total_purchases_by_items.purchase_year = to_char(now(), 'YYYY')
                     ORDER BY purchases_35_plus.total_sum_for_month_for_users_35_plus DESC,
                              total_purchases_by_items.total DESC
                     LIMIT 1"""
    
    second_query = """SELECT total_for_years.itemid, (total_for_years.total * 100) / sum(total_for_years.total) OVER() as share_in_revenue_for_the_year
                      FROM (
                            SELECT i.itemid, sum(i.price) as total, to_char(p."date", 'YYYY') as purchase_year
                            FROM purchases p
                            JOIN items i ON p.itemid = i.itemid
                            GROUP BY i.itemid, purchase_year
                      ) as total_for_years
                      WHERE total_for_years.purchase_year = to_char(now(), 'YYYY')
                      ORDER BY total_for_years.total DESC
                      LIMIT 3"""
        
    cursor.execute(first_query)
    first_data_mart_data = cursor.fetchall()

    cursor.execute(second_query)
    second_data_mart_data = cursor.fetchall()

    return first_data_mart_data, second_data_mart_data


def prepare_data_marts_tables(cursor):
    create_first_data_mart_query = """CREATE TABLE IF NOT EXISTS main_sales_metrics (
                                        between_18_and_25_prices_avg NUMERIC(6, 2) NOT NULL,
                                        between_26_and_35_prices_avg NUMERIC(6, 2) NOT NULL,
                                        month_that_had_max_revenue VARCHAR(2) NOT NULL,
                                        item_id_that_generates_the_maximum_income_for_the_year INTEGER NOT NULL
                                    )"""
    create_second_data_mart_query = """CREATE TABLE IF NOT EXISTS top_items_for_current_year (
                                           itemId INTEGER NOT NULL,
                                           share_in_revenue_for_the_year NUMERIC(5, 2) NOT NULL
                                        )"""
    cursor.execute(create_first_data_mart_query)
    cursor.execute(create_second_data_mart_query)


def insert_data_to_data_marts(cursor, first_data_mart_data, second_data_mart_data):
    insert_first_data_mart_query = """INSERT INTO main_sales_metrics (between_18_and_25_prices_avg,
                                                                      between_26_and_35_prices_avg,
                                                                      month_that_had_max_revenue,
                                                                      item_id_that_generates_the_maximum_income_for_the_year)
                                      VALUES (%s, %s, %s, %s)"""
    
    insert_second_data_mart_query = """INSERT INTO top_items_for_current_year (itemId,
                                                                               share_in_revenue_for_the_year)
                                       VALUES (%s, %s)"""
    
    cursor.executemany(insert_first_data_mart_query, first_data_mart_data)
    cursor.executemany(insert_second_data_mart_query, second_data_mart_data)


def extract_and_load_data_mart():
    connection = psycopg2.connect(**db_connection_data)
    try:
        with connection:
            with connection.cursor() as cur:
                first_data_mart_data, second_data_mart_data = select_data_from_db(cur)

        with connection:
            with connection.cursor() as cur:
                prepare_data_marts_tables(cur)

        with connection:
            with connection.cursor() as cur:
                insert_data_to_data_marts(cur, first_data_mart_data, second_data_mart_data)

    finally:
        connection.close()


with DAG(
    'create_datamarts',
    catchup=False,
    schedule='0 0 * * *',
    default_args=args
):

    task1 = BashOperator(
        task_id='echo',
        bash_command='echo {text}'.format(text='"I am starting to complete the daily task of creating data marts"')
    )

    task2 = PythonOperator(
        task_id='extract_and_load_data_mart',
        python_callable=extract_and_load_data_mart
    )

    task1 >> task2
