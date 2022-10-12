import os
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime, timedelta


DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def create_connection():
    connection = None
    try:
        connection = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        print(f"Connection to PostgreSQL DB calls_cost successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f'The error {e} occurred')


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error {e} occurred")


def moscow_time(dt):
    """Добавляет 3 часа к дате-времени чтобы получить московское время"""
    try:
        no_tz = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        no_tz = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%SZ')
    moscow_tz = no_tz + timedelta(hours=3)
    return moscow_tz


# --------------------СОЗДАНИЕ----------------------------------------------
connection_new_db = psycopg2.connect(
    database=os.getenv('postgres'),
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
)
# Создать базу
# create_database_query = f"CREATE DATABASE {DB_NAME}"
# create_database(connection_new_db, create_database_query)


# Подключиться к базе
connection = create_connection()

# Создать таблицу
create_clients_table = """
CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    manager TEXT,
    active BOOLEAN DEFAULT '1',
    teleph_id TEXT UNIQUE,
    autoru_id INTEGER UNIQUE,
    avito_id INTEGER UNIQUE,
    drom_id INTEGER UNIQUE
)
"""
execute_query(connection, create_clients_table)

create_marks_table = """
CREATE TABLE IF NOT EXISTS marks (
    id SERIAL PRIMARY KEY,
    mark TEXT NOT NULL UNIQUE,
    teleph TEXT UNIQUE,
    autoru TEXT UNIQUE,
    avito TEXT UNIQUE,
    drom TEXT UNIQUE,
    human_name TEXT
)
"""
execute_query(connection, create_marks_table)
create_models_table = """
CREATE TABLE IF NOT EXISTS models (
    id SERIAL,
    mark_id INTEGER NOT NULL,
    model TEXT NOT NULL,
    teleph TEXT,
    autoru TEXT,
    avito TEXT,
    drom TEXT,
    human_name TEXT,
    CONSTRAINT models_pkey PRIMARY KEY (id, mark_id)
)
"""
execute_query(connection, create_models_table)

create_autoru_calls_table = """
CREATE TABLE IF NOT EXISTS autoru_calls (
    id SERIAL PRIMARY KEY,
    ad_id TEXT,
    vin TEXT,
    client_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    duration INTEGER NOT NULL,
    mark TEXT,
    model TEXT,
    billing_state TEXT,
    billing_cost FLOAT
)
"""
execute_query(connection, create_autoru_calls_table)

create_autoru_products_table = """
CREATE TABLE IF NOT EXISTS autoru_products (
    id SERIAL PRIMARY KEY,
    ad_id TEXT,
    vin TEXT,
    client_id INTEGER NOT NULL,
    date DATE NOT NULL,
    mark TEXT,
    model TEXT,
    product TEXT NOT NULL,
    sum FLOAT NOT NULL,
    count INTEGER NOT NULL,
    total FLOAT NOT NULL
)
"""
execute_query(connection, create_autoru_products_table)

# TODO добавить mark
create_teleph_calls_table = """
CREATE TABLE IF NOT EXISTS teleph_calls (
    id SERIAL PRIMARY KEY,
    client_id TEXT,
    datetime TIMESTAMPTZ NOT NULL,
    num_from TEXT NOT NULL,
    mark TEXT,
    model TEXT,
    target TEXT,
    moderation TEXT,
    call_price FLOAT,
    price_autoru FLOAT,
    price_drom FLOAT
)
"""
execute_query(connection, create_teleph_calls_table)


# Создать связи
# teleph_calls_clients_fk = """
# ALTER TABLE teleph_calls
# ADD CONSTRAINT teleph_calls_clients_fk
# FOREIGN KEY (client_id) REFERENCES clients(teleph_id)
# """
# execute_query(connection, teleph_calls_clients_fk)

# clients_autoru_calls_fk = """
# ALTER TABLE autoru_calls
# ADD CONSTRAINT autoru_calls_clients_fk
# FOREIGN KEY (client_id) REFERENCES clients(autoru_id)
# """
# execute_query(connection, clients_autoru_calls_fk)

# clients_autoru_products_fk = """
# ALTER TABLE autoru_products
# ADD CONSTRAINT autoru_products_clients_fk
# FOREIGN KEY (client_id) REFERENCES clients(autoru_id)
# """
# execute_query(connection, clients_autoru_products_fk)

# models_marks_fk = """
# ALTER TABLE models
# ADD CONSTRAINT models_marks_fk
# FOREIGN KEY (mark_id) REFERENCES marks(id)
# """
# execute_query(connection, models_marks_fk)


# --------------------ДОБАВЛЕНИЕ (INSERT)----------------------------------------------
def add_autoru_products(data):
    # Каждая новая запись должна быть tuple
    autoru_products = []
    for offer in data['offer_product_activations_stats']:
        for stat in range(len(offer['stats'])):
            sum = offer['stats'][stat]['sum']
            count = offer['stats'][stat]['count']
            total = sum * count
            if total > 0:  # Добавляю только те услуги за которые списали средства
                ad_id = offer['offer']['id']
                vin = offer['offer']['documents']['vin']
                client_id = int(offer['offer']['user_ref'] \
                                .split(':')[1])
                date = offer['stats'][stat]['date']
                mark = offer['offer']['car_info']['mark_info']['name']
                model = offer['offer']['car_info']['model_info']['name']
                product = offer['stats'][stat]['product']

                # Проверяю есть ли уже эта запись
                product_unique_check = f"""
                SELECT COUNT (*) FROM autoru_products  
                WHERE ad_id = '{ad_id}' AND date = '{date}' AND product = '{product}'
                """
                select_count = execute_read_query(connection, product_unique_check)
                if select_count[0][0] == 0:
                    autoru_products.append((ad_id, vin, client_id, date, mark, model, product, sum, count, total))

    if len(autoru_products) > 0:
        # Это даст %s, %s, %s, %s, %s, %s, %s, %s для каждой новой записи (placeholder)
        autoru_products_records = ", ".join(["%s"] * len(autoru_products))
        # А это после VALUES добавит %s, %s, %s, %s, %s, %s, %s, %s
        insert_query = (
            f"INSERT INTO autoru_products (ad_id, vin, client_id, date, mark, model, product, sum, count, total) "
            f"VALUES {autoru_products_records}"
        )

        connection.autocommit = True
        cursor = connection.cursor()
        # И по итогу .execute() выполнит SQL запрос
        # INSERT INTO users (name, age, gender, nationality) VALUES %s, %s, %s, %s, %s, %s, %s, %s
        # Где вместо каждой %s будут поочередно подставляться данные из users, например:
        # INSERT INTO autoru_products (ad_id, client_id, date, mark, model, product, sum, count)
        # VALUES ("1116406727-64a30494", "WAUZZZGYXNA047922", 50877, "2022-08-31", "EXEED", "TXL", "premium", 200, 1, 200)
        cursor.execute(insert_query, autoru_products)


def add_autoru_daily(data, client):
    placements_filter = ['quota:placement:cars:used', 'quota:placement:cars:new', 'quota:placement:commercial',
                         'trade-in-request:cars:used', 'trade-in-request:cars:new']
    autoru_daily = []
    try:
        for day in data['activation_stats']:
            if day['product'] in placements_filter:
                ad_id = 'null'
                vin = 'null'
                client_id = client
                date = day['date']
                mark = 'null'
                model = 'null'
                product = day['product']
                sum = day['sum']
                count = day['count']
                total = sum * count

                # Проверяю есть ли уже эта запись
                daily_unique_check = f"""
                SELECT COUNT (*) FROM autoru_products  
                WHERE client_id = '{client}' AND date = '{date}' AND product = '{product}'
                """
                select_count = execute_read_query(connection, daily_unique_check)
                if select_count[0][0] == 0:
                    autoru_daily.append((ad_id, vin, client_id, date, mark, model, product, sum, count, total))
    except KeyError:
        print(f'Клиент {client} пропущен. {data}')
        return

    if len(autoru_daily) > 0:
        autoru_daily_records = ", ".join(["%s"] * len(autoru_daily))
        insert_query = (
            f"INSERT INTO autoru_products (ad_id, vin, client_id, date, mark, model, product, sum, count, total) "
            f"VALUES {autoru_daily_records}"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, autoru_daily)
        print(f'Клиент {client}. Расходы за размещения добавлены')


def add_autoru_calls(data, client):
    autoru_calls = []
    for call in data['calls']:
        try:
            ad_id = call['offer']['id']
        except KeyError:
            ad_id = 'null'
        try:
            vin = call['offer']['documents']['vin']
        except KeyError:
            vin = 'null'
        try:
            mark = call['offer']['car_info']['mark_info']['name']
        except KeyError:
            mark = 'Другое'
        try:
            model = call['offer']['car_info']['model_info']['name']
        except KeyError:
            model = 'Другое'
        try:
            duration = call['call_duration']['seconds']
        except KeyError:
            duration = 0
        try:
            billing_state = call['billing']['state']
        except KeyError:
            billing_state = 'FREE'
        client_id = client
        source = call['source']['raw']
        target = call['target']['raw']
        datetime = moscow_time(call['timestamp'])
        if billing_state == 'PAID':
            billing_cost = int(call['billing']['cost']['amount']) / 100
        elif billing_state == 'FREE':
            billing_cost = 0

        call_unique_check = f"""
        SELECT COUNT (*) FROM autoru_calls 
        WHERE source = '{source}' AND target = '{target}' AND datetime = '{datetime}'
        """
        select_count = execute_read_query(connection, call_unique_check)
        if select_count[0][0] == 0:
            autoru_calls.append((ad_id, vin, client_id, source, target, datetime, duration,
                                 mark, model, billing_state, billing_cost))
    if len(autoru_calls) > 0:
        autoru_calls_records = ', '.join(['%s'] * len(autoru_calls))
        insert_query = (f"""
        INSERT INTO autoru_calls (ad_id, vin, client_id, source, target, datetime, duration, 
                                  mark, model, billing_state, billing_cost)
        VALUES {autoru_calls_records}
        """)

        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, autoru_calls)


def add_teleph_calls(data, client):
    teleph_calls = []
    for call in data['content']:
        client_id = client
        datetime = call['dateTime']
        num_from = call['numFrom']
        mark = call['mark']
        model = call['model']
        target = call['target']
        moderation = call['moderation']
        call_price = call['callPrice']
        price_autoru = call['priceAutoRu']
        price_drom = call['priceDrom']

        call_unique_check = f"""
        SELECT COUNT (*) FROM teleph_calls
        WHERE client_id = '{client}' AND datetime = '{datetime}' AND num_from = '{num_from}'
        """
        select_count = execute_read_query(connection, call_unique_check)
        if select_count[0][0] == 0:
            teleph_calls.append((client_id, datetime, num_from, mark, model, target,
                                 moderation, call_price, price_autoru, price_drom))
    if len(teleph_calls) > 0:
        teleph_calls_records = ', '.join(['%s'] * len(teleph_calls))
        insert_query = (
            f'INSERT INTO teleph_calls (client_id, datetime, num_from, mark, model, target, '
            f'                             moderation, call_price, price_autoru, price_drom)'
            f'VALUES {teleph_calls_records}'
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, teleph_calls)


# --------------------ВЫБОРКА (SELECT)----------------------------------------------
def select_autoru_clients_active():
    query = """
    SELECT autoru_id FROM clients WHERE active = true AND autoru_id IS NOT NULL
    """
    result = execute_read_query(connection, query)
    ids = [result[i][0] for i in range(len(result))]
    return ids


def select_teleph_clients_active():
    query = """
    SELECT teleph_id FROM clients WHERE active = true AND teleph_id IS NOT NULL
    """
    result = execute_read_query(connection, query)
    client_ids = [result[i][0] for i in range(len(result))]
    return client_ids
