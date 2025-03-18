import scrapy
import json
import re
import os
from scrapy.crawler import CrawlerProcess
from datetime import datetime
from urllib.parse import urljoin


class SobDetailSpider(scrapy.Spider):
    name = "sob_detail"

    def __init__(self, *args, **kwargs):
        super(SobDetailSpider, self).__init__(*args, **kwargs)
        self.results_dir = 'results'
        self.details_file = 'sob_details.json'
        self.details_path = os.path.join(self.results_dir, self.details_file)

        # Создаем директорию для результатов, если её нет
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

        # Загружаем существующие данные или создаем пустой список
        self.existing_details = []
        self.existing_urls = set()
        if os.path.exists(self.details_path):
            try:
                with open(self.details_path, 'r', encoding='utf-8') as f:
                    self.existing_details = json.load(f)
                    # Создаем множество уже обработанных URL для быстрой проверки
                    self.existing_urls = {item['url'] for item in self.existing_details if 'url' in item}
                    self.logger.info(f"Загружено {len(self.existing_details)} существующих записей")
            except Exception as e:
                self.logger.error(f"Ошибка при чтении файла {self.details_file}: {e}")

        # Загружаем ссылки на объявления из всех JSON файлов в папке results
        self.ad_urls = []
        if os.path.exists(self.results_dir):
            for filename in os.listdir(self.results_dir):
                if filename.endswith('.json') and filename != self.details_file:
                    try:
                        filepath = os.path.join(self.results_dir, filename)
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            for ad in data:
                                if 'url' in ad and ad['url'] not in self.existing_urls and ad[
                                    'url'] not in self.ad_urls:
                                    self.ad_urls.append(ad['url'])
                    except Exception as e:
                        self.logger.error(f"Ошибка при чтении файла {filename}: {e}")

        self.logger.info(f"Загружено {len(self.ad_urls)} новых ссылок на объявления")

    def start_requests(self):
        # Обрабатываем каждую ссылку на объявление
        for url in self.ad_urls:
            if url not in self.existing_urls:
                yield scrapy.Request(url, callback=self.parse_detail)
            else:
                self.logger.info(f"Пропуск уже обработанного URL: {url}")

    def parse_detail(self, response):
        self.logger.info(f"Парсинг деталей объявления: {response.url}")

        # Извлекаем все требуемые данные
        ad_detail = {
            'url': response.url,
            'title': self.get_title(response),
            'price': self.get_price(response),
            'phone': self.get_phone(response),
            'is_agency': self.check_if_agency(response),
            'region': "Московский",
            'city': self.get_city(response),
            'address': self.get_address(response),
            'description': self.get_description(response),
            'deal_type': self.get_deal_type(response),
            'images': self.get_images(response),
            'categories': self.get_categories(response),
            'attributes': self.get_attributes(response),
            'publication_date': self.get_publication_date(response)
        }

        # Добавляем новую запись и сохраняем результаты
        self.existing_details.append(ad_detail)
        self.existing_urls.add(response.url)

        # Сохраняем после каждой обработанной записи
        with open(self.details_path, 'w', encoding='utf-8') as f:
            json.dump(self.existing_details, f, ensure_ascii=False, indent=4)

        self.logger.info(f"Детали объявления сохранены. Всего записей: {len(self.existing_details)}")


    def get_title(self, response):
            # Заголовок объявления
            title = response.css('div.adv-page-title h1::text').get()
            return title.strip() if title else ""

    def get_price(self, response):
        # Цена в объявлении
        price = response.css('p.text-price::text').get()
        if price:
            # Удаляем все нецифровые символы
            return re.sub(r'[^\d]', '', price.strip())
        return ""

    def get_phone(self, response):
        # Номер телефона
        phone = response.css('div.phone-show-visible p b::text').get()
        if phone:
            # Форматируем номер телефона
            return "7" + re.sub(r'[^\d]', '', phone)
        return ""

    def check_if_agency(self, response):
        # Компания/агентство или частное лицо
        agent_info = response.css('div.b-card-info-default__2column__agent::text').getall()
        agent_info = ' '.join([text.strip() for text in agent_info if text.strip()])

        if 'Агентство' in agent_info or 'Компания' in agent_info:
            return 1
        return 0

    def get_city(self, response):
        # Ищем ссылку на город в формате "Город г."
        city_link = response.css('div.flex-two-equals a.black-link::text').get()

        if city_link and ' г.' in city_link:
            # Возвращаем название города без "г."
            return city_link.replace(' г.', '').strip()

        # Если город не найден
        return "Москва"
    def get_address(self, response):
        # Адрес (улица, дом)
        address_links = response.css('div.adv-page-content1 p a.black-link::text').getall()
        if address_links:
            return ' '.join([link.strip() for link in address_links])

        # Альтернативный способ получения адреса
        address = response.css('div.adv-page-content1 p:contains("Адрес")::text').get()
        if address:
            return address.strip()

        return "Не указано"

    def get_description(self, response):
        # Описание объявления
        description = response.css('div.adv-page-desc p::text').getall()
        if description:
            return ' '.join([p.strip() for p in description if p.strip()])
        return "Не указано"

    def get_deal_type(self, response):
        # Проверяем наличие информации о сроке аренды
        rent_term = response.css('div.flex-four-equals p:contains("Срок аренды")::text').get()
        if rent_term:
            return 3  # Сдам (аренда)

        # Проверяем наличие информации о продаже
        deal_info = response.css('div.flex-four-equals p:contains("Сделка")::text').get()
        if deal_info and ('продажа' in deal_info.lower() or 'прямая продажа' in deal_info.lower()):
            return 2  # Продам

        # Если не удалось определить из текста, пытаемся определить из URL
        if 'prodazha' in response.url:
            return 2
        elif 'arenda' in response.url:
            return 3

        return 0  # Неизвестно

    def get_images(self, response):
        # Массив ссылок на изображения
        images = []

        # Ищем все ссылки на изображения с помощью регулярного выражения
        img_pattern = r'//images\.sob\.ru/[^"\']+'
        img_urls = re.findall(img_pattern, response.text)

        # Обрабатываем найденные URL изображений
        for img_src in img_urls:
            if img_src and 'sob.ru' in img_src:
                # Заменяем размер изображения на 1920x1080
                full_size_src = re.sub(r'_\d+x\d+', '_1920x1080', img_src)

                # Добавляем протокол
                if full_size_src.startswith('//'):
                    full_size_src = 'https:' + full_size_src

                # Избегаем дубликатов
                if full_size_src not in images:
                    images.append(full_size_src)

        return images

    def get_categories(self, response):
        # Категории из хлебных крошек
        categories = []
        breadcrumbs = response.css('ul.breadcrumbs li a')

        for item in breadcrumbs:
            href = item.css('::attr(href)').get()
            title = item.css('span::text').get()

            if href and title:
                categories.append({
                    'href': urljoin(response.url, href),
                    'title': title.strip()
                })

        return categories[1:]

    def get_attributes(self, response):
        # Характеристики/атрибуты объявления
        attributes = {}

        # Парсим все блоки с характеристиками
        attribute_blocks = response.css('div.adv-page-content2 div.flex-four-equals div')

        for block in attribute_blocks:
            attributes_in_block = block.css('p')

            for attr in attributes_in_block:
                attr_text = attr.get()
                if attr_text:
                    # Извлекаем название и значение атрибута
                    attr_name_match = re.search(r'<b>([^<]+)</b>', attr_text)

                    if attr_name_match:
                        attr_name = attr_name_match.group(1).strip().rstrip(':')

                        # Извлекаем значение, удаляя HTML теги
                        attr_value = re.sub(r'<[^>]+>', '', attr_text.split('</b>', 1)[1])
                        attr_value = attr_value.strip()

                        attributes[attr_name] = attr_value

        return attributes

    def get_publication_date(self, response):
        # Дата публикации
        date_text = response.css('div.flex-two-equals p.text-date:contains("Дата публикации")::text').get()

        if date_text:
            # Извлекаем дату из текста вида "Дата публикации: 11 июня 2024"
            date_match = re.search(r'Дата публикации: (\d{1,2}) (\w+) (\d{4})', date_text)
            if date_match:
                day = date_match.group(1).zfill(2)
                month_name = date_match.group(2).lower()
                year = date_match.group(3)

                # Словарь для перевода русских названий месяцев в числа
                ru_month_map = {
                    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                    'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                    'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
                }

                if month_name in ru_month_map:
                    month = ru_month_map[month_name]
                    # Возвращаем в формате ISO 8601 (YYYY-MM-DD)
                    return f"{year}-{month}-{day}"

        return None  # Возвращаем None, если дата не найдена

class SobAdsSpider(scrapy.Spider):
    name = "sob_ads"

    def __init__(self, *args, **kwargs):
        super(SobAdsSpider, self).__init__(*args, **kwargs)
        self.start_urls = []

        # Создаем директорию для сохранения результатов, если её нет
        if not os.path.exists('results'):
            os.makedirs('results')

        # Загружаем URL из JSON
        try:
            with open('sob_menu_links.json', 'r', encoding='utf-8') as f:
                self.menu_data = json.load(f)

                # Составляем список всех URL
                for category in self.menu_data:
                    for item in self.menu_data[category]:
                        self.start_urls.append(item['url'])

                if not self.start_urls:
                    self.logger.error("В JSON файле не найдено ссылок")
                else:
                    self.logger.info(f"Загружено {len(self.start_urls)} ссылок из JSON")

        except Exception as e:
            self.logger.error(f"Ошибка при загрузке JSON: {e}")

    def start_requests(self):
        # Обрабатываем каждую ссылку из JSON
        for url in self.start_urls:
            # Начинаем с первой страницы
            yield scrapy.Request(url, callback=self.parse, meta={'page': 1, 'base_url': url, 'ads_count': 0})

    def parse(self, response):
        current_page = response.meta.get('page', 1)
        base_url = response.meta.get('base_url', response.url)
        ads_count = response.meta.get('ads_count', 0)
        square_from = response.meta.get('square_from', None)
        square_to = response.meta.get('square_to', None)

        self.logger.info(f"Парсинг страницы {current_page} для {base_url}")

        # Проверяем количество страниц в пагинации
        last_page_link = response.css('a[rel="last"] b::text').get()

        # Если это первая страница и есть пагинация с 30+ страницами, разделяем поиск
        if current_page == 1 and last_page_link and int(last_page_link) >= 30:
            # Если параметры площади еще не установлены
            if square_from is None and square_to is None:
                # Устанавливаем начальные значения
                square_from = 1
                square_to = 999

                # Формируем URL с параметрами площади
                if '?' in base_url:
                    next_url = f"{base_url}&total_square_from={square_from}&total_square_to={square_to}"
                else:
                    next_url = f"{base_url}?total_square_from={square_from}&total_square_to={square_to}"

                self.logger.info(f"Слишком много результатов, добавляем фильтр по площади: {square_from}-{square_to}")
                yield scrapy.Request(
                    next_url,
                    callback=self.parse,
                    meta={'page': 1, 'base_url': base_url, 'ads_count': ads_count,
                          'square_from': square_from, 'square_to': square_to}
                )
                return

            # Если параметры площади уже установлены, но результатов все еще много
            elif last_page_link and int(last_page_link) >= 30 and square_to > square_from:
                # Уменьшаем верхнюю границу вдвое
                new_square_to = square_to // 2

                if new_square_to <= square_from:
                    new_square_to = square_from + 1

                # Формируем URL с обновленными параметрами
                if '?' in base_url:
                    next_url = f"{base_url}&total_square_from={square_from}&total_square_to={new_square_to}"
                else:
                    next_url = f"{base_url}?total_square_from={square_from}&total_square_to={new_square_to}"

                self.logger.info(f"Все еще много результатов, сужаем диапазон площади: {square_from}-{new_square_to}")
                yield scrapy.Request(
                    next_url,
                    callback=self.parse,
                    meta={'page': 1, 'base_url': base_url, 'ads_count': ads_count,
                          'square_from': square_from, 'square_to': new_square_to}
                )
                return

        # Если количество страниц меньше 30 и у нас установлены параметры площади
        # Проверяем, нужно ли сдвинуть нижнюю границу
        elif current_page == 1 and square_from is not None and square_to is not None and last_page_link and int(
                last_page_link) < 30:
            # Переходим к следующему диапазону площади
            new_square_from = square_to + 1
            new_square_to = 999

            # Формируем URL для следующего диапазона
            if '?' in base_url:
                next_url = f"{base_url}&total_square_from={new_square_from}&total_square_to={new_square_to}"
            else:
                next_url = f"{base_url}?total_square_from={new_square_from}&total_square_to={new_square_to}"

            self.logger.info(f"Переходим к следующему диапазону площади: {new_square_from}-{new_square_to}")
            yield scrapy.Request(
                next_url,
                callback=self.parse,
                meta={'page': 1, 'base_url': base_url, 'ads_count': ads_count,
                      'square_from': new_square_from, 'square_to': new_square_to}
            )

        # Парсим объявления
        ads = []

        # Находим все элементы с классом grid-search-content
        grid_items = response.css('.grid-search-content')

        # Если на странице нет объявлений и у нас установлены параметры площади
        if not grid_items and square_from is not None and square_to is not None:
            # Если текущий диапазон не дал результатов, переходим к следующему URL из start_urls
            self.logger.info(
                f"Нет объявлений для диапазона площади {square_from}-{square_to}, переходим к следующей ссылке")
            return

        # Если на странице нет объявлений, прекращаем пагинацию
        if not grid_items:
            self.logger.info(f"На странице {current_page} для {base_url} не найдено объявлений. Прекращаем пагинацию.")
            return

        self.logger.info(f"Найдено {len(grid_items)} элементов grid-search-content")

        # Счетчик объявлений на текущей странице
        page_ads_count = 0

        for item in grid_items:
            # Находим ссылки с классом title-adv
            title_links = item.css('.title-adv')
            for link in title_links:
                href = link.css('::attr(href)').get()
                title = link.css('::text').get()

                ads.append({
                    'title': title.strip() if title else '',
                    'url': response.urljoin(href),
                })
                page_ads_count += 1
                self.logger.info(f"Найдено объявление: {title.strip() if title else ''} - {href}")

        # Если на странице были объявления, сохраняем их
        if page_ads_count > 0:
            # Формируем имя файла на основе URL
            url_part = base_url.split('/')[-1].split('?')[0]

            # Добавляем информацию о диапазоне площади в имя файла, если он установлен
            square_info = ""
            if square_from is not None and square_to is not None:
                square_info = f"_sq{square_from}-{square_to}"

            filename = f"results/ads_{url_part}{square_info}_page_{current_page}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(ads, f, ensure_ascii=False, indent=4)

            self.logger.info(f"Сохранено {page_ads_count} объявлений в файл {filename}")

            # Обновляем общий счетчик объявлений
            ads_count += page_ads_count

            # Определяем последнюю часть функции parse, которая обрабатывает пагинацию

            # Проверяем, есть ли следующая страница
            if current_page < 30:
                next_page = current_page + 1

                # Формируем URL следующей страницы, сохраняя параметры площади, если они есть
                next_url = base_url

                # Добавляем параметры площади, если они заданы
                if square_from is not None and square_to is not None:
                    if '?' in next_url:
                        next_url = f"{next_url}&total_square_from={square_from}&total_square_to={square_to}"
                    else:
                        next_url = f"{next_url}?total_square_from={square_from}&total_square_to={square_to}"

                # Добавляем номер страницы
                if '?' in next_url:
                    next_url = f"{next_url}&page={next_page}"
                else:
                    next_url = f"{next_url}?page={next_page}"

                self.logger.info(f"Переход к странице {next_page}: {next_url}")
                yield scrapy.Request(
                    next_url,
                    callback=self.parse,
                    meta={
                        'page': next_page,
                        'base_url': base_url,
                        'ads_count': ads_count,
                        'square_from': square_from,
                        'square_to': square_to
                    }
                )
            else:
                # Если достигли предела пагинации
                square_info = ""
                if square_from is not None and square_to is not None:
                    square_info = f" для диапазона площади {square_from}-{square_to}"

                self.logger.info(
                    f"Достигнут предел пагинации (30) для {base_url}{square_info}. Всего собрано {ads_count} объявлений.")
        else:
            # Если на странице не было объявлений
            self.logger.info(f"На странице {current_page} для {base_url} не найдено объявлений. Прекращаем пагинацию.")


class SobMenuSpider(scrapy.Spider):
    name = "sob_menu"
    start_urls = ["https://sob.ru/"]

    def parse(self, response):
        # Сначала проверим, загрузилась ли страница вообще
        self.logger.info(f"Заголовок страницы: {response.css('title::text').get()}")

        # Ищем меню по классу более универсально
        header_menu = response.xpath('//ul[contains(@class, "header-menu-list")]')
        self.logger.info(f"Найдено header-menu-list: {len(header_menu)}")

        # Если меню найдено, парсим его
        if header_menu:
            # Находим все пункты меню
            menu_items = header_menu.css('li')
            self.logger.info(f"Найдено {len(menu_items)} пунктов меню")

            results = {}

            for item in menu_items:
                menu_text = item.css('a::text').get()
                if menu_text:
                    menu_text = menu_text.strip()
                    self.logger.info(f"Найден пункт меню: {menu_text}")

                    if menu_text in ['Продажа', 'Аренда']:
                        results[menu_text] = []

                        # Ищем все ссылки в подменю
                        links = item.css('a[href]')
                        self.logger.info(f"В разделе {menu_text} найдено {len(links)} ссылок")

                        for link in links:
                            url = link.css('::attr(href)').get()
                            title = link.css('::text').get()

                            if title:
                                title = title.strip()

                                # Определяем регион из контекста
                                parent_ul = link.xpath('./parent::li/parent::ul')
                                region_text = parent_ul.xpath('./li[1]/text()').get()
                                region = region_text.strip() if region_text else "Неизвестно"

                                link_data = {
                                    'title': title,
                                    'url': response.urljoin(url),
                                    'region': region
                                }

                                results[menu_text].append(link_data)
                                self.logger.info(f"Добавлена ссылка: {title} - {url}")

            # Сохраняем результаты
            with open('sob_menu_links.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=4)

            return results
        else:
            self.logger.error("Меню не найдено!")

# Запускаем паук напрямую
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'LOG_LEVEL': 'INFO',
        'CONCURRENT_REQUESTS': 1,  # Ограничиваем количество одновременных запросов
        'DOWNLOAD_DELAY': 2,  # Добавляем задержку между запросами в секундах
        'ROBOTSTXT_OBEY': False  # Отключаем соблюдение robots.txt для тестирования
    })

    # Выберите, какой паук запустить
    # process.crawl(SobMenuSpider)  # Для сбора меню
    process.crawl(SobAdsSpider)  # Для сбора объявлений
    process.crawl(SobDetailSpider)  # Для сбора деталей объявлений
    process.start()