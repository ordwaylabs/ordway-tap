import requests
import ordway_tap.configs


def get_index_data(path, params):
    page = 1
    pending_result = True
    while pending_result:
        results = get(path + '?size=20&page=' + str(page), params)
        if len(results) == 0:
            pending_result = False
        page = page + 1
        for result in results:
            yield result


def get(path, params):
    response = requests.get(get_url(path), headers=get_headers(), params=params)
    if response.status_code != 200:
        response.raise_for_status()

    return response.json()


def get_headers():
    api_credentials = ordway_tap.configs.api_credentials
    return {
        'X-Company-Token': api_credentials['x_company_token'],
        'X-User-Company': api_credentials['x_company'],
        'X-User-Token': api_credentials['x_user_token'],
        'X-User-Email': api_credentials['x_user_email'],
        'X-API-KEY': api_credentials['x_api_key']
    }


def get_url(path):
    api_credentials = ordway_tap.configs.api_credentials
    return api_credentials['endpoint'] + path
