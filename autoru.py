import os
import requests
import json
from datetime import datetime, timedelta
import time
from database import add_autoru_products, add_autoru_daily, add_autoru_calls, select_autoru_clients_active

ENDPOINT = 'https://apiauto.ru/1.0'

TOTAL_REQUESTS = 0

API_KEY = {
    'x-authorization': os.getenv('AUTORU_API_KEY'),
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}


def autoru_authenticate():
    # Аутентификация пользователя
    auth = '/auth/login'
    login_pass = {
        'login': os.getenv('AUTORU_LOGIN'),
        'password': os.getenv('AUTORU_PASSWORD')
    }
    auth_response = requests.post(url=f'{ENDPOINT}{auth}', headers=API_KEY, json=login_pass)
    session_id = {'x-session-id': auth_response.json()['session']['id']}
    return session_id


def get_autoru_clients():
    active_clients_ids = select_autoru_clients_active()
    return active_clients_ids


def get_autoru_products(from_, to, client_id):
    # Возвращает статистику по активации услуги у объявлений за указанную дату.
    # https://yandex.ru/dev/autoru/doc/reference/dealer-wallet-product-activations-offer-stats.html
    # GET /dealer/wallet/product/{productName}/activations/offer-stats
    global TOTAL_REQUESTS
    # TODO не идти по всем услугам, сравнивать сумму за день с общей суммой услуг (get_autoru_daily) и останавливать по достижении
    product_types = ['placement', 'premium', 'special-offer', 'boost', 'badge', 'turbo-package', 'reset']
    dealer_headers = {**API_KEY, **session_id, 'x-dealer-id': f'{client_id}'}
    date_start = datetime.strptime(from_, '%Y-%m-%d')
    date_end = datetime.strptime(to, '%Y-%m-%d')
    current_date = date_start
    # За каждый день собираю статистику по всем видам услуг
    while current_date <= date_end:
        for product_type in product_types:
            start = time.perf_counter()
            TOTAL_REQUESTS += 1
            if TOTAL_REQUESTS % 290 == 0:
                print('290ый запрос')
                time.sleep(60)
            product_params = {
                'service': 'autoru',
                'date': f'{current_date:%Y-%m-%d}',
                'pageNum': 1,
                'pageSize': 80
            }
            product_response = requests.get(
                url=f'{ENDPOINT}/dealer/wallet/product/{product_type}/activations/offer-stats',
                headers=dealer_headers, params=product_params).json()
            try:
                if product_response['offer_product_activations_stats']:
                    add_autoru_products(product_response)

                page_count = product_response['paging']['page_count']
                if page_count > 1:
                    for page in range(2, page_count + 1):
                        product_params['pageNum'] = page
                        product_response = requests.get(
                            url=f'{ENDPOINT}/dealer/wallet/product/{product_type}/activations/offer-stats',
                            headers=dealer_headers, params=product_params).json()
                        add_autoru_products(product_response)
            except KeyError:
                continue
            finally:
                print(f'Клиент {client_id}, услуги, запрос {TOTAL_REQUESTS:4} | {time.perf_counter() - start:.6f}')
        current_date += timedelta(days=1)
    print('Перерыв')
    time.sleep(60)  # Жду минуту после последнего запроса чтобы не словить ограничение авто.ру
    TOTAL_REQUESTS = 0


def get_autoru_calls(from_, to, client_id):
    # Возвращает список звонков дилера.
    # https://yandex.ru/dev/autoru/doc/reference/calltracking.html
    # POST /calltracking
    calltracking = 'calltracking'
    dealer_headers = {**API_KEY, **session_id, 'x-dealer-id': f'{client_id}'}
    calls_body = {
        "pagination": {
            "page": 1,
            "page_size": 100
        },
        "filter": {
            "period": {
                "from": f'{from_}T00:00:00.000Z',
                "to": f'{to}T23:59:59.000Z'
            },
            "targets": 'ALL_TARGET_GROUP',
            # "results": 'ALL_RESULT_GROUP',
            # "callbacks": 'ALL_SOURCE_GROUP',
            # "unique": 'ALL_UNIQUE_GROUP',
            "category": [
                'CARS'
            ],
            "section": [
                'NEW', 'USED'
            ],
        },
        "sorting": {
            "sorting_field": 'CALL_TIME',
            "sorting_type": 'ASCENDING'
        }
    }
    calls_response = requests.post(url=f'{ENDPOINT}/{calltracking}', headers=dealer_headers, json=calls_body).json()
    try:
        if calls_response['status'] == 'error':  # Пропускаю клиента если доступ запрещён
            print(f'Клиент {client_id} пропущен. Отказано в доступе')
            return
    except KeyError:
        if calls_response['pagination']['total_count'] > 0:
            add_autoru_calls(calls_response, client_id)
            # Если запрос возвращает больше одной страницы то иду по следующим
            page_count = calls_response['pagination']['total_page_count']
            if page_count > 1:
                for page in range(2, page_count + 1):
                    calls_body['pagination']['page'] = page
                    calls_response = requests.post(url=f'{ENDPOINT}/{calltracking}', headers=dealer_headers,
                                                   json=calls_body).json()
                    add_autoru_calls(calls_response, client_id)


def get_autoru_daily(from_, to, client_id):
    # Списание с кошелька за звонки и активацию услуг
    # https://yandex.ru/dev/autoru/doc/reference/dealer-wallet-product-activations-daily-stats.html
    # GET /dealer/wallet/product/activations/daily-stats
    wallet = '/dealer/wallet/product/activations/daily-stats'
    dealer_headers = {**API_KEY, **session_id, 'x-dealer-id': f'{client_id}'}
    wallet_params = {
        'service': 'autoru',
        'from': from_,
        'to': to,
        'pageNum': 1,
        'pageSize': 1000
    }
    wallet_response = requests.get(url=f'{ENDPOINT}{wallet}', headers=dealer_headers, params=wallet_params).json()
    add_autoru_daily(wallet_response, client_id)


session_id = autoru_authenticate()
