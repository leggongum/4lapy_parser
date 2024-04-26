import aiohttp
from bs4 import BeautifulSoup

import asyncio
from ast import literal_eval
import csv
import json

URL = 'https://4lapy.ru/catalog'

PRODUCTS = []
OFFERS_LINK_MAP = {}


def fill_csv(data: dict):
    with open('result.csv', 'a', encoding='UTF-8', newline='') as f:
        fieldnames = ['id', 'title', 'link', 'price', 'promo_price', 'brand']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        #writer.writeheader(fieldnames)

        for id_, product in data.items():
            for id_, offer in product['offers'].items():
                if not offer['available'] or not OFFERS_LINK_MAP.get(int(id_)): continue
                name = offer['ecommerce'].split("'name':'")[1].split("','brand'")[0]
                brand = offer['ecommerce'].split("'brand':'")[1].split("','category'")[0]
                row = {
                    'id': id_, 
                    'title': literal_eval(f"'{name}'"),
                    'link': OFFERS_LINK_MAP[int(id_)],
                    'price': offer['oldPrice'],
                    'promo_price': offer['price'],
                    'brand': literal_eval(f"'{brand}'")
                }

                writer.writerow(row)


async def get_products_info(session: aiohttp.ClientSession, products):
    url = 'https://4lapy.ru/ajax/catalog/product-info/'
    params = {
        'section_id': '3',
        'sort': 'popular',
        'product[]': products,
    }
    async with session.get(url, params=params) as resp:
        if resp.status == 524:
            return await get_products_info(session, products)
        return json.loads((await resp.content.read()).decode())



def parse_html_page(content):
    soup = BeautifulSoup(content, 'lxml')

    product_list = [offer['data-productid'] for offer in soup.find_all('div', class_='b-common-item b-common-item--catalog-item js-product-item')]
    PRODUCTS.extend(product_list)

    offers_links = {int(offer.find('a')['data-offerid']): offer.find('a')['data-link'] for offer in soup.find_all('li', class_='b-weight-container__item')}
    OFFERS_LINK_MAP.update(offers_links)
    try:
        return int(soup.find_all('a', class_='b-pagination__link js-pagination')[-2]['title'])
    except IndexError:
        print(product_list, offers_links)


async def get_content(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        print(resp.status, url)
        if resp.status == 404:
            raise Exception('The catalog is not exist')
        elif resp.status > 299:
            print('Something went wrong...')
            await asyncio.sleep(2)
            return await get_content(session, url)
        else:
            return await resp.text()

async def check_categoria(session: aiohttp.ClientSession, url: str):
    return parse_html_page(await get_content(session, url))


async def get_offers_from_page(session: aiohttp.ClientSession, url: str, num):
    url += f'&page={num}'
    parse_html_page(await get_content(session, url))


async def main(categoria: str) -> None:
    url = URL + categoria
    session = aiohttp.ClientSession()
    try:
        pages_amount = await check_categoria(session, url)
        tasks = [get_offers_from_page(session, url, i) for i in range(2, pages_amount+1)]
        await asyncio.gather(*tasks)

        tasks = []
        global PRODUCTS
        while PRODUCTS:
            tasks.append(get_products_info(session, PRODUCTS[:50]))
            PRODUCTS = PRODUCTS[50:]

        for res in asyncio.as_completed(tasks):
            fill_csv((await res)['data']['products'])


    except Exception as ex:
        raise ex

    finally:
        await session.close()



if __name__ == '__main__':
    categoria = '/koshki/korm-koshki/sukhoy/?section_id=3&sort=popular'
    asyncio.run(main(categoria or input('Input categoria: ')))
