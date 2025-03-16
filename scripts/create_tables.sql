-- Создание таблицы user_sessions
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR(50),
    user_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    device VARCHAR(50),
    pages_visited_count INT,
    actions_count INT,
    PRIMARY KEY (session_id, start_time) -- Добавлен start_time в первичный ключ
) PARTITION BY RANGE (start_time); -- Партиционирование по start_time

-- Создание таблицы product_price_history
CREATE TABLE IF NOT EXISTS product_price_history (
    product_id VARCHAR(50) PRIMARY KEY,
    current_price NUMERIC,
    currency VARCHAR(10),
    price_change_count INT
);

-- Создание таблицы support_tickets
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id VARCHAR(50),
    user_id INT,
    status VARCHAR(20),
    issue_type VARCHAR(50),
    messages_count INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (ticket_id, created_at) -- Добавлен created_at в первичный ключ
) PARTITION BY RANGE (created_at); -- Партиционирование по created_at

CREATE TABLE IF NOT EXISTS session_stats (
    user_id INT PRIMARY KEY,
    avg_session_duration INTERVAL,
    total_pages_visited INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ticket_status_summary (
    status VARCHAR(20) PRIMARY KEY,
    ticket_count INT,
    avg_resolution_time INTERVAL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
