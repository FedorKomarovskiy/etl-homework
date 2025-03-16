from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['etl_db']

# Генерация UserSessions
def generate_user_sessions(n=100):
    sessions = []
    for _ in range(n):
        start_time = fake.date_time_this_year()
        end_time = start_time + timedelta(minutes=random.randint(5, 60))
        session = {
            'session_id': fake.uuid4(),
            'user_id': random.randint(1, 50),
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'pages_visited': [fake.uri() for _ in range(random.randint(1, 10))],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'actions': [fake.word() for _ in range(random.randint(1, 5))]
        }
        sessions.append(session)
    db.user_sessions.insert_many(sessions)

# Генерация ProductPriceHistory
def generate_price_history(n=100):
    histories = []
    for _ in range(n):
        price_changes = [
            {'date': fake.date_this_year().isoformat(), 'price': round(random.uniform(10, 1000), 2)}
            for _ in range(random.randint(1, 5))
        ]
        history = {
            'product_id': fake.uuid4(),
            'price_changes': price_changes,
            'current_price': price_changes[-1]['price'],
            'currency': 'USD'
        }
        histories.append(history)
    db.product_price_history.insert_many(histories)

# Генерация SupportTickets
def generate_support_tickets(n=100):
    tickets = []
    for _ in range(n):
        created_at = fake.date_time_this_year()
        updated_at = created_at + timedelta(days=random.randint(0, 5))
        ticket = {
            'ticket_id': fake.uuid4(),
            'user_id': random.randint(1, 50),
            'status': random.choice(['open', 'closed', 'in_progress']),
            'issue_type': fake.word(),
            'messages': [fake.sentence() for _ in range(random.randint(1, 3))],
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        tickets.append(ticket)
    db.support_tickets.insert_many(tickets)

if __name__ == '__main__':
    db.user_sessions.drop()
    db.product_price_history.drop()
    db.support_tickets.drop()
    generate_user_sessions(100)
    generate_price_history(100)
    generate_support_tickets(100)
    print("Data generation completed! Check MongoDB at 'etl_db'.")
