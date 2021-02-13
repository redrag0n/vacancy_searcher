import json
import time
import logging
import urllib.request

from urllib.error import HTTPError
from datetime import datetime, timedelta, timezone


VACANCY_URL = 'https://api.hh.ru/vacancies'
HH_PAGE_LIMIT = 2000
TIME_STEP = {'minutes': 30}
TIME_PERIOD = {'days': 1}
TIME_LOG_PERIOD = {'hours': 1}
OUTPUT_DIR = '../data'


def get_response_body(request):
    with urllib.request.urlopen(request) as response:
        page_dict = json.loads(response.read().decode('utf8'))
        return page_dict


def get_all_pages(url):
    vacancy_dict_list = list()
    page_number = 0
    page_count = None
    while page_count is None or page_number < page_count:
        page_url = f'{url}&page={page_number}&per_page=100'
        request = urllib.request.Request(page_url)
        try:
            page_dict = get_response_body(request)
        except HTTPError as err:
            break
        if len(page_dict['items']) == 0:
            break
        if page_number == 0:
            page_count = page_dict['pages']

        vacancy_dict_list.extend(page_dict['items'])
        page_number += 1
    return vacancy_dict_list


def get_page_list_for_time_range(url, current_time_up, current_time_down):
    date_to = urllib.parse.quote_plus(current_time_up.isoformat(timespec='seconds'))
    date_from = urllib.parse.quote_plus(current_time_down.isoformat(timespec='seconds'))
    period_url = f'{url}&date_from={date_from}&date_to={date_to}'
    period_dict_list = get_all_pages(period_url)
    if len(period_dict_list) >= HH_PAGE_LIMIT:
        logging.warning(f'for time {current_time_up.isoformat()}: {len(period_dict_list)} vacancies')
    return period_dict_list


def log(vacancy_dict_list, prev_log_time, next_log_time):
    output_fn = f'{OUTPUT_DIR}/vacancies{prev_log_time}--{next_log_time}.json'
    with open(output_fn, mode='w', encoding='utf8') as out:
        json.dump(vacancy_dict_list, out, ensure_ascii=False)
    prev_time = prev_log_time.isoformat()
    next_time = next_log_time.isoformat()
    logging.info(f'for time {prev_time} - {next_time}: {len(vacancy_dict_list)} vacancies')


def get_vacancies_by_search_phrase(text):
    url = f'{VACANCY_URL}?text={text}'

    start_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    end_time = start_time - timedelta(**TIME_PERIOD)

    prev_log_time = start_time
    log_time_delta = timedelta(**TIME_LOG_PERIOD)
    next_log_time = start_time - log_time_delta

    current_time_up = start_time
    step_time_delta = timedelta(**TIME_STEP)
    current_time_down = current_time_up - step_time_delta

    vacancy_dict_list = list()
    page_count = 0
    vacancy_count = 0
    while current_time_up > end_time:
        while current_time_up > next_log_time:
            period_dict_list = get_page_list_for_time_range(url, current_time_up, current_time_down)
            vacancy_dict_list.extend(period_dict_list)
            page_count += 1
            current_time_up = current_time_down
            current_time_down -= step_time_delta

        log(vacancy_dict_list, prev_log_time, next_log_time)
        prev_log_time = next_log_time
        next_log_time -= log_time_delta
        vacancy_count += len(vacancy_dict_list)
        vacancy_dict_list = list()
    return vacancy_count


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    time_begin = time.time()
    vacancy_count = get_vacancies_by_search_phrase(text='developer')
    print(f'vacancy_count: {vacancy_count}')
    time_end = time.time()
    print(f'time: {time_end - time_begin}')

