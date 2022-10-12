import os
import requests
from database import add_teleph_calls, select_teleph_clients_active

TELEPH_ENDPOINT = 'https://151.248.121.33:9000/api/calls'
TELEPH_TOKEN = {
    'token': os.getenv('TELEPH_TOKEN')
}


def get_teleph_clients():
    active_clients_ids = select_teleph_clients_active()
    return active_clients_ids


def get_teleph_calls(from_, to, client_login):
    params = {
        'login': client_login,
        'dateFrom': from_,
        'dateTo': to,
        'pageSize': 1000
    }
    teleph_response = requests.get(url=TELEPH_ENDPOINT, headers=TELEPH_TOKEN,
                                   params=params, verify=False).json()
    add_teleph_calls(teleph_response, client_login)
    page_count = teleph_response['totalPages']
    if page_count > 1:
        for page in range(1, page_count - 1):
            params['pageNumber'] = page
            teleph_response = requests.get(url=TELEPH_ENDPOINT, headers=TELEPH_TOKEN,
                                           params=params, verify=False).json()
            add_teleph_calls(teleph_response, client_login)
