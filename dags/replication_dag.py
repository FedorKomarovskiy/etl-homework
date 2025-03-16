from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import psycopg2
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 16),
    'retries': 1,
}

def extract_transform_load():
    # Подключение к MongoDB
    mongo_client = MongoClient('mongodb://mongodb:27017/')
    db = mongo_client['etl_db']

    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        dbname='airflow', user='airflow', password='airflow', host='postgres', port=5432
    )
    cursor = pg_conn.cursor()

    # Репликация UserSessions
    sessions = list(db.user_sessions.find())
    for session in sessions:
        # Трансформация: убираем дубликаты по session_id, считаем количество страниц и действий
        cursor.execute("""
            INSERT INTO user_sessions (session_id, user_id, start_time, end_time, device, pages_visited_count, actions_count)
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM user_sessions WHERE session_id = %s)
            ON CONFLICT (session_id) DO NOTHING
        """, (
            session['session_id'], session['user_id'], session['start_time'],
            session['end_time'], session['device'], len(session['pages_visited']),
            len(session['actions']), session['session_id']
        ))

    # Репликация ProductPriceHistory
    histories = list(db.product_price_history.find())
    for history in histories:
        cursor.execute("""
            INSERT INTO product_price_history (product_id, current_price, currency, price_change_count)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM product_price_history WHERE product_id = %s)
            ON CONFLICT (product_id) DO NOTHING
        """, (
            history['product_id'], history['current_price'], history['currency'],
            len(history['price_changes']), history['product_id']
        ))

    # Репликация SupportTickets
    tickets = list(db.support_tickets.find())
    for ticket in tickets:
        cursor.execute("""
            INSERT INTO support_tickets (ticket_id, user_id, status, issue_type, messages_count, created_at, updated_at)
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM support_tickets WHERE ticket_id = %s)
            ON CONFLICT (ticket_id) DO NOTHING
        """, (
            ticket['ticket_id'], ticket['user_id'], ticket['status'], ticket['issue_type'],
            len(ticket['messages']), ticket['created_at'], ticket['updated_at'],
            ticket['ticket_id']
        ))

    pg_conn.commit()
    cursor.close()
    pg_conn.close()
    logging.info(f"Replicated {len(sessions)} sessions, {len(histories)} histories, {len(tickets)} tickets")

with DAG('replication_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_transform_load
    )
