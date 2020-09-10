from asyncio import run, gather
from aiohttp import ClientSession
from lxml.html import fromstring, tostring
from json_dump import open_df
from html2text import html2text
from csv import writer, QUOTE_NONNUMERIC
from json import load
from tqdm import tqdm

NUMS_THREADS = 10
RESULT_FILE = 'avitek-i.ru.csv'
DUMP_FILE = 'avitek-i.ru.json'


async def get_page(url, **kwargs):
    """Асинхронное получение кода страницы"""
    async with ClientSession() as sess:
        async with sess.get(url, **kwargs) as req:
            return await req.text(errors='replace')


async def get_all_catalog_urls():
    """Получение url'ов всех каталогов"""
    url = 'https://avitek-i.ru/catalog/'

    content_page = await get_page(url)

    document = fromstring(content_page)

    urls = document.xpath('//td[@class="section_info"]/ul/li[@class="name"]/a/@href')

    for i in range(len(urls)):
        urls[i] = 'https://avitek-i.ru' + urls[i]

    return urls


async def get_product_urls_from_page(url_page):
    """Получение url'ов товаров со страницы каталога"""
    content_page = await get_page(url_page)

    document = fromstring(content_page)

    urls = document.xpath('//div[@class="item-title"]/a/@href')

    for i in range(len(urls)):
        urls[i] = 'https://avitek-i.ru' + urls[i]

    return urls


async def get_catalog_length(catalog_url):
    """Возвращает номер самой последней страницы каталога"""
    content_page = await get_page(catalog_url)

    document = fromstring(content_page)

    length = document.xpath('//div[@class="nums"]/a[last()]/text()')
    if length:
        return int(length[-1])
    else:
        return 1


async def get_all_product_urls_from_catalog(catalog_url):
    """Получение всех url'ов товаров со всех страниц каталога"""
    catalog_length = await get_catalog_length(catalog_url)
    urls = []

    answers = await gather(
        *[get_product_urls_from_page(catalog_url + '?PAGEN_1={}'.format(i)) for i in range(1, catalog_length + 1)])
    for answer in answers:
        urls += answer

    return urls


async def get_product_data(url):
    """Парсинг карточки товара"""
    data = {'url': url}

    try:
        content_page = await get_page(url)
    except:
        return False

    document = fromstring(content_page)

    image = document.xpath('//li[@id="photo-0"]/a/@href')
    if image:
        data['Фото товара'] = 'https://avitek-i.ru' + image[0]

    name = document.xpath('//h1[@id="pagetitle"]/text()')
    if name:
        data['Название товара'] = name[0]

    price = document.xpath('//div[@class="price"]/@data-value')
    if price:
        data['Цена'] = price[0]

    article = document.xpath('//div[@class="article iblock"]/span[@class="value"]/text()')
    if article:
        data['Артикул'] = article[0]

    description = document.xpath('//div[@class="tabs_section"]/ul/li[@class=" current"]/div[@class="detail_text"]')
    if description:
        data['Описание'] = html2text('\n'.join(description[0].xpath('../div[@class="detail_text"]//text()'))).\
            replace('\r', '').replace('\t', '')
        data['Описание (html)'] = tostring(description[0]).decode()

    breadcrumbs = document.xpath('//div[@class="breadcrumbs"]/div/a/span[@itemprop="name"]/text()')
    data['Хлебные крошки'] = ' > '.join(breadcrumbs[:-1]).replace('\r', '').replace('\t', '').replace('\n', '')

    characteristics_name = document.xpath('//li/table/tr/td[@class="char_name"]/span/span/text()')
    characteristics_value = document.xpath('//li/table/tr/td[@class="char_value"]/span/text()')

    for i in range(len(characteristics_name)):
        if len(characteristics_value) > i:
            data[characteristics_name[i]] = characteristics_value[i].replace('\n', '').replace('\r', '').replace('\t', '')

    return data


async def get_all_product_urls():
    """Получение всех url'ов товаров с сайта"""
    catalog_urls = await get_all_catalog_urls()

    product_urls = []
    for catalog_url in catalog_urls:
        product_urls += await get_all_product_urls_from_catalog(catalog_url)

    return product_urls


async def main():
    product_urls = await get_all_product_urls()

    file_dump = open_df(DUMP_FILE)
    for i in tqdm(range(0, len(product_urls), NUMS_THREADS)):
        urls = product_urls[i: i + NUMS_THREADS] if i + NUMS_THREADS < len(product_urls) else product_urls[i:]
        answers = await gather(*[get_product_data(url) for url in urls])

        for answer in answers:
            if answer:
                file_dump.write(answer)
    file_dump.close()

    with open(DUMP_FILE, 'r') as file:
        write_to_csv(load(file))


def write_to_csv(data_products):
    """Запись словаря в csv в csv"""
    default_characteristics = {}

    all_characteristics_name = []
    for product in data_products:  # Получение списка ВСЕХ возможных характеристик
        for name in product.keys():
            if name not in all_characteristics_name:
                all_characteristics_name.append(name)
                default_characteristics[name] = ''

    for i in range(len(data_products)):  # Добавление ВСЕХ характеристик к каждому продукту
        dh = default_characteristics.copy()
        dh.update(data_products[i])
        data_products[i] = dh

    with open(RESULT_FILE, 'w') as file:  # Запись в csv файл
        csv_writer = writer(file, delimiter=';',quoting=QUOTE_NONNUMERIC)
        data = []
        for value in data_products[0].keys():
            data.append(value)

        data_products[0].update({'Каталог': ''})
        csv_writer.writerow(data_products[0].keys())

        for product in data_products:
            product['Цена'] = product['Цена'].split('.')[0]
            product.update({'Каталог': product['Хлебные крошки'].split('>')[-1]})
            csv_writer.writerow(product.values())


if __name__ == '__main__':
    #run(main())
    with open(DUMP_FILE, 'r') as file:
        write_to_csv(load(file))
