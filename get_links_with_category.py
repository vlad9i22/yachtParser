import requests
import json

from bs4 import BeautifulSoup

from retry_requests import retry



catalog_url = 'https://yacht-parts.ru/catalog/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}


def get_all_categories_links():
    session = retry()
    r = session.get(catalog_url, headers=headers)
    print(r.status_code)
    soup = BeautifulSoup(r.content, 'html5lib')

    category_links = []

    for subsect in soup.find_all('ul', {'class': 'subsections'}):
        for link in subsect.find_all('a'):
            if not link.string:
                continue
            cur_link = link.get('href')
            category = link.string.strip()
            if cur_link and cur_link.startswith("/catalog"):
                category_links.append((cur_link, category))
    return category_links

def get_all_items_from_page(url, page_num):
    payload = {'PAGEN_1': str(page_num)}
    session = retry()
    r = session.get(url, headers=headers, params=payload)
    print(r.status_code)
    if r.status_code != 200: 
        return []
    soup = BeautifulSoup(r.content, 'html5lib')
    items = soup.find_all('div', {'class' : 'item-title'})
    links = []
    r = session.get(catalog_url, headers=headers, params=payload)

    for link in items:
        links.append(link.find('a').get('href'))
    return links

