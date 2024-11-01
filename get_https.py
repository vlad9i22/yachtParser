import requests
import re
import logging
import os
from itertools import chain

import pandas as pd

from bs4 import BeautifulSoup
from retry_requests import retry

from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock

from get_links_with_category import get_all_categories_links, get_all_items_from_page 
RAW_URL = "https://yacht-parts.ru"
logger = logging.getLogger("get_https")
logging.basicConfig(filename='processed.log', encoding='utf-8', level="INFO")

lock = Lock()
cnt = 0

def get_price(soup):
    if soup.find_all('div', {"class": "price"}):
        return soup.find_all('div', {"class": "price"})[0].string.strip()
    return "no price"


def get_name(soup):
    return soup.find_all('h1', {"id": "pagetitle"})[0].string.strip()


def get_description(soup):
    res = ""
    preview = soup.find('div', {"class": "preview_text"})
    if preview and preview.string: 
        res += preview.string
    #for desc in soup.find('div', {"class": "detail_text"}).find_all('p'):
    #    if desc.string and len(desc.string) > 30:
    #        res += desc.string
    return res


def get_images_of_goods(soup):
    images = []
    for div in soup.find_all('div', {"class": "item_slider"}):
        for img in div.find_all('img'):
            images.append(RAW_URL + img.attrs['src'])
    return set(images)


def get_brand_name(soup):
    brand_block = soup.find('div', {"class": "brand iblock"})
    if not brand_block:
        return ""
    return brand_block.find("a", {"class": "brand_picture"}).find("img").attrs['alt']


def get_article(soup):
    if not soup.find('div', {"class": "article iblock"}):
        return ""
    return soup.find('div', {"class": "article iblock"}).find("span", {"class": "value"}).string


def get_number_of_pages(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}
    session = retry()
    r = session.get(url, headers=headers)
    print("Getting numbers")
    soup = BeautifulSoup(r.content, 'html5lib')
    if not soup.find('span', {'class': 'nums'}):
        return "1"
    return soup.find('span', {'class': 'nums'}).find_all('a')[-1].string


def process_one_item(item_url, category):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}
    session = retry()
    r = session.get(item_url, headers=headers)
    print(r.status_code)
    soup = BeautifulSoup(r.content, 'html5lib')
    d = {}
    d["category"] = str(category)
    d["Articule"] = str(get_article(soup))
    d["Brand"] = str(get_brand_name(soup))
    d["Item_name"] = str(get_name(soup))
    d["Price"] = str(get_price(soup))
    d["Description"] = str(get_description(soup))
    d["Images"] = str(", ".join(get_images_of_goods(soup)))
    soup.decompose()
    with lock:
        global cnt
        cnt += 1
        print(f"processed {cnt} pages in {category}")
    return pd.DataFrame([d])


def to_num(page_num):
    if page_num.isnumeric():
        return int(page_num)
    return 1

def get_processed_categories():
    return [s[0:-5].replace('___' , '/') for s in os.listdir("./categories_tables")]


def save_table(d, category):
    res_table = pd.DataFrame(data=d)
    res_table.drop_duplicates()
    category = category.replace('/', '___')
    category_path = "./categories_tables/" + category + ".xlsx"
    print(f"Saving to {category_path}")
    res_table.to_excel(category_path)

def process():
    d = {"category": [], "Articule": [], "Brand" : [], "Item_name": [], "Price": [], "Description": [], "Images": []}
    category_list = get_all_categories_links()
    processed_cat = get_processed_categories()
    for cat_link, category in category_list:
        if category in processed_cat:
            continue
        with lock:
            global cnt
            cnt = 0
        print(f"Processing {category}")
        output = pd.DataFrame()
        num_of_pages = to_num(get_number_of_pages(RAW_URL + cat_link))
        # Get all pages from category
        items = []
        params = [(RAW_URL + cat_link, page_num) for page_num in range(1, num_of_pages + 1)]
        with ThreadPool(40) as pool:
            items = pool.starmap(get_all_items_from_page, params)
        items = list(chain.from_iterable(items))
        print(f"All pages links: {items}")
        # Get data from all pages
        params = [(RAW_URL + url, category) for url in items]
        with ThreadPool(40) as pool:
            pages = pool.starmap(process_one_item, params)
        if pages:
            output_page = pd.concat(pages, ignore_index=True)
            output = pd.concat([output, output_page])
        save_table(output, category)
        logger.info(f"Processed: {category}")


if __name__ == "__main__":
    process()

    
