import os
import time
from datetime import date
import calendar
from autoru import get_autoru_clients, get_autoru_calls, get_autoru_products, get_autoru_daily
from teleph import get_teleph_clients, get_teleph_calls
from database import create_connection, execute_read_query
from flask import Flask, render_template, request, redirect, url_for
from flask_bootstrap import Bootstrap


FROM = '2022-08-01'
TO =   '2022-08-31'

start_app = time.perf_counter()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET')
Bootstrap(app)


@app.route('/', methods=['GET', 'POST'])
def home():
    # Список клиентов
    connection = create_connection()
    get_clients_query = """
        SELECT * FROM clients
        WHERE active
        ORDER BY name
        """
    clients = execute_read_query(connection, get_clients_query)
    connection.close()

    # Таблица себестоимости
    if request.method == 'POST':
        daterange = request.form['daterange']
        datefrom = daterange.split(' ')[0]
        dateto = daterange.split(' ')[2]

        clients_checked = request.form.getlist('client-checkbox')
        clients_ids = ', '.join(clients_checked)
        connection = create_connection()
        # TODO ниже запрос только по авто.ру
        get_stats_query = f"""
        SELECT
            clients.name AS Клиент, 
            COALESCE(teleph_calls_sum, 0) AS "Приход с площадки",
            COALESCE(calls_sum, 0) + COALESCE(products_sum, 0) AS "Траты на площадку", 
            COALESCE(calls_sum, 0) AS "Траты на звонки", 
            COALESCE(products_sum, 0) AS "Траты на услуги",
            COALESCE(teleph_target, 0) AS "Звонки с площадки",
            round(COALESCE((COALESCE(calls_sum, 0) + COALESCE(products_sum, 0)) / teleph_target, 0)::DECIMAL, 2)::TEXT AS "Цена звонка",
            round(COALESCE(COALESCE(teleph_calls_sum, 0) / teleph_target, 0)::DECIMAL, 2)::TEXT AS "Цена клиента",
            round((COALESCE(COALESCE(teleph_calls_sum, 0) / teleph_target, 0) - COALESCE((COALESCE(calls_sum, 0) + COALESCE(products_sum, 0)) / teleph_target, 0))::DECIMAL, 2)::TEXT AS "Маржа",
            round((COALESCE(teleph_target, 0) * (COALESCE(COALESCE(teleph_calls_sum, 0) / teleph_target, 0) - COALESCE((COALESCE(calls_sum, 0) + COALESCE(products_sum, 0)) / teleph_target, 0)))::DECIMAL, 2)::TEXT AS "Заработок"        
        FROM (SELECT id, name, autoru_id, teleph_id
            FROM clients) clients
            FULL JOIN
                (SELECT client_id, SUM(billing_cost) AS calls_sum
                FROM autoru_calls
                WHERE (datetime >= '{datefrom} 00:00:00' AND datetime <= '{dateto} 23:59:59')
                GROUP BY client_id) calls
            ON (clients.autoru_id = calls.client_id)
            FULL JOIN
                (SELECT client_id, SUM(total) AS products_sum
                FROM autoru_products
                WHERE (date >= '{datefrom}' AND date <= '{dateto}')
                GROUP BY client_id) products
            ON (clients.autoru_id = products.client_id)
            FULL JOIN
                (SELECT client_id, SUM(call_price) AS teleph_calls_sum, COUNT(target) AS teleph_target
                FROM teleph_calls
                WHERE (datetime >= '{datefrom} 00:00:00' AND datetime <= '{dateto} 23:59:59')
                    AND (target = 'Да' OR target = 'ПМ - Целевой')
                    AND moderation LIKE 'М%'
                GROUP BY client_id) teleph_calls
            ON (clients.teleph_id = teleph_calls.client_id)
        WHERE clients.id IN ({clients_ids})
        ORDER BY Клиент
        """
        stats = execute_read_query(connection, get_stats_query)
        connection.close()
        return render_template('index.html', clients=clients, clients_checked=clients_checked,
                               stats=stats, datefrom=datefrom, dateto=dateto)

    # Первое и последнее число текущего месяца. Передаётся при первом открытии index.html
    today = date.today()
    year = today.year
    month = today.month
    datefrom = date(year, month, 1).strftime('%d.%m.%Y')
    dateto = date(year, month, calendar.monthrange(year, month)[1]).strftime('%d.%m.%Y')

    return render_template('index.html', clients=clients, datefrom=datefrom, dateto=dateto)


active_autoru_clients_ids = get_autoru_clients()

# # Собираю звонки по всем клиентам за указанный период
# for i in range(len(active_autoru_clients_ids)):
#     start = time.perf_counter()
#     get_autoru_calls(FROM, TO, active_autoru_clients_ids[i])
#     if i != 0:
#         if i % 290 == 0:  # Ждать минуту каждый 290ый запрос (у авто.ру ограничение не более 300 запросов в минуту)
#             print('290ый запрос')
#             time.sleep(60)
#     print(f'Клиент {active_autoru_clients_ids[i]}, звонки, запрос {i:3} | {time.perf_counter() - start:.6f}')

# # Собираю услуги по всем клиентам за указанный период
# for i in range(len(active_autoru_clients_ids)):
#     get_autoru_products(FROM, TO, active_autoru_clients_ids[i])

# # Собираю списания за размещения объявлений
# for i in range(len(active_autoru_clients_ids)):
#     get_autoru_daily(FROM, TO, active_autoru_clients_ids[i])


# Собираю звонки с телефонии по всем клиентам
# active_teleph_clients = get_teleph_clients()
# for client in active_teleph_clients:
#     get_teleph_calls(FROM, TO, client)

print(f'{time.perf_counter() - start_app:.6f}')

if __name__ == '__main__':
    app.run(debug=True)
