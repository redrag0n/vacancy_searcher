import os
import json
import time
import logging
import aiohttp
import urllib.request
import asyncio

from datetime import datetime, timedelta, timezone
from datetimerange import DateTimeRange


VACANCY_URL = 'https://api.hh.ru/vacancies'
HH_PAGE_LIMIT = 2000
TIME_STEP = {'minutes': 60}
TIME_PERIOD = {'days': 31}
TIME_LOG_PERIOD = {'minutes': 360}
OUTPUT_DIR = '../data'
WAIT_REQUEST_TIME_LIMIT = 30
MAX_COUNT_ASYNC_REQUEST = 500


async def get_page_data_coro(url, session):
    wait_time = 1
    while True:
        try:
            async with session.get(url) as response:
                response_dict = await response.json()
                if 'errors' in response_dict:
                    await asyncio.sleep(wait_time)
                    wait_time *= 2
                    continue
                break
        except (aiohttp.client_exceptions.ClientConnectorError,
                aiohttp.client_exceptions.ServerDisconnectedError,
                aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ClientPayloadError,
                asyncio.exceptions.TimeoutError) as err:
            await asyncio.sleep(wait_time)
            wait_time *= 2
            continue
    return response_dict


async def get_vacancy_coro(vacancy_id, session):
    url = f'{VACANCY_URL}/{vacancy_id}'
    return await get_page_data_coro(url, session)


async def get_vacancies_coro(vacancy_id_list):
    vacancy_list = list()
    async with aiohttp.ClientSession() as session:
        task_batches = [[asyncio.create_task(get_vacancy_coro(vacancy_id, session))
                         for vacancy_id in vacancy_id_list[i: i + MAX_COUNT_ASYNC_REQUEST]]
                        for i in range(0, len(vacancy_id_list), MAX_COUNT_ASYNC_REQUEST)]
        for task_batch in task_batches:
            vacancy_batch = await asyncio.gather(*task_batch)
            vacancy_list.extend(vacancy_batch)
        return vacancy_list


def get_period_url(url, current_time_up, current_time_down):
    date_to = urllib.parse.quote_plus(current_time_up.isoformat(timespec='seconds'))
    date_from = urllib.parse.quote_plus(current_time_down.isoformat(timespec='seconds'))
    period_url = f'{url}&date_from={date_from}&date_to={date_to}'
    return period_url


async def get_search_page_vacancies_async(url, time_interval, session):
    url = get_period_url(url, *time_interval)
    search_page = await get_page_data_coro(url, session)
    return [vac['id'] for vac in search_page['items']]


def get_page_number_url(url, page_number):
    page_url = f'{url}&page={page_number}&per_page=100'
    return page_url


async def get_vacancy_ids_for_time_interval_coro(url, time_interval, session):
    url = get_period_url(url, *time_interval)
    first_search_page = await get_page_data_coro(get_page_number_url(url, 0), session)
    page_count = first_search_page['pages']
    tasks = [asyncio.create_task(get_page_data_coro(get_page_number_url(url, page), session))
             for page in range(1, page_count)]
    search_page_list = await asyncio.gather(*tasks)
    search_page_list = [first_search_page] + search_page_list
    vacancy_id_list = [vac['id'] for vac in sum([page['items'] for page in search_page_list], [])]
    return vacancy_id_list


async def get_search_res_list(url, time_interval_list):
    """Итерация по всем временным мини-интревалам"""
    vacancy_id_list = list()
    async with aiohttp.ClientSession() as session:
        task_batches = [[asyncio.create_task(get_vacancy_ids_for_time_interval_coro(url, interval, session))
                        for interval in time_interval_list[i: i + MAX_COUNT_ASYNC_REQUEST]]
                        for i in range(0, len(time_interval_list), MAX_COUNT_ASYNC_REQUEST)]
        for task_batch in task_batches:
            vacancy_batch = await asyncio.gather(*task_batch)
            vacancy_id_list.extend(sum(vacancy_batch, []))
    return vacancy_id_list


def get_vacancies_by_search_phrase_async2(search_dict):
    search_str = '&'.join(f'{k}={v}' for k, v in search_dict.items())
    url = f'{VACANCY_URL}?{search_str}'
    print(url)

    start_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    end_time = start_time - timedelta(**TIME_PERIOD)

    step_time_delta = timedelta(**TIME_STEP)
    time_range = DateTimeRange(end_time, start_time)
    time_interval_list = [(start + step_time_delta, start)
                          for start in time_range.range(step_time_delta)][::-1]
    vacancy_id_list = asyncio.run(get_search_res_list(url, time_interval_list))
    vacancy_list = asyncio.run(get_vacancies_coro(list(set(vacancy_id_list))))

    with open(f'{OUTPUT_DIR}/{search_str}{start_time}{end_time}.json', mode='w', encoding='utf8') as out:
        json.dump(vacancy_list, out, ensure_ascii=False)
    return len(vacancy_list)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    time_begin = time.time()
    search_dict = {'specialization': '1.221'}
    vacancy_count = get_vacancies_by_search_phrase_async2(search_dict)
    print(f'vacancy_count: {vacancy_count}')
    time_end = time.time()
    print(f'time: {time_end - time_begin}')

