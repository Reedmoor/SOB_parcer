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
        self.details_dir = 'details'

        # Создаем директорию для деталей, если её нет
        if not os.path.exists(self.details_dir):
            os.makedirs(self.details_dir)

        # Загружаем ссылки на объявления из всех JSON файлов в папке results
        self.ad_urls = []
        if os.path.exists(self.results_dir):
            for filename in os.listdir(self.results_dir):
                if filename.endswith('.json'):
                    try:
                        filepath = os.path.join(self.results_dir, filename)
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            for ad in data:
                                if 'url' in ad and ad['url'] not in self.ad_urls:
                                    self.ad_urls.append(ad['url'])
                    except Exception as e:
                        self.logger.error(f"Ошибка при чтении файла {filename}: {e}")

        self.logger.info(f"Загружено {len(self.ad_urls)} ссылок на объявления")

    def start_requests(self):
        # Обрабатываем каждую ссылку на объявление
        for url in self.ad_urls:
            yield scrapy.Request(url, callback=self.parse_detail)

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

        # Формируем имя файла на основе ID объявления из URL
        ad_id = response.url.split('/')[-1]
        if not ad_id:
            ad_id = response.url.split('/')[-2]

        filename = f"{self.details_dir}/ad_{ad_id}.json"

        # Сохраняем результаты
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(ad_detail, f, ensure_ascii=False, indent=4)

        self.logger.info(f"Детали объявления сохранены в файл {filename}")

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
        # Добавляем import re для работы с регулярными выражениями
        import re

        # Обрабатываем каждую ссылку из JSON
        for url in self.start_urls:
            # Для каждой ссылки начинаем с широкого диапазона площади
            min_square = 1
            max_square = 999

            # Добавляем параметры площади к URL
            if '?' in url:
                url_with_params = f"{url}&total_square_from={min_square}&total_square_to={max_square}"
            else:
                url_with_params = f"{url}?total_square_from={min_square}&total_square_to={max_square}"

            # Начинаем с первой страницы
            yield scrapy.Request(
                url_with_params,
                callback=self.check_range,
                meta={
                    'base_url': url,
                    'square_range': {'from': min_square, 'to': max_square},
                    'adjusting_range': True,  # Флаг, показывающий, что мы настраиваем диапазон
                    'fine_tuning': False  # Флаг для тонкой настройки верхней границы
                }
            )

    def check_range(self, response):
        """Проверяет текущий диапазон площади и корректирует при необходимости"""
        base_url = response.meta.get('base_url')
        square_range = response.meta.get('square_range')
        previous_good = response.meta.get('previous_good', None)
        phase = response.meta.get('phase', 'initial')  # Фазы: initial, expand, binary_search

        self.logger.info(f"Проверка диапазона: от {square_range['from']} до {square_range['to']} м² для {base_url}")

        # Проверяем наличие элемента пагинации с rel="last"
        last_page_link = response.css('a[rel="last"]')
        too_many_pages = False
        last_page_number = 0

        if last_page_link:
            href = last_page_link.css('::attr(href)').get()
            page_match = re.search(r'page=(\d+)', href)
            if page_match:
                last_page_number = int(page_match.group(1))
                if last_page_number >= 30:
                    too_many_pages = True
                    self.logger.info(f"Обнаружена пагинация с последней страницей {last_page_number} (≥30)")

        # Формируем следующий URL
        def make_url(from_val, to_val):
            if '?' in base_url:
                return f"{base_url}&total_square_from={from_val}&total_square_to={to_val}"
            else:
                return f"{base_url}?total_square_from={from_val}&total_square_to={to_val}"

        if phase == 'initial':
            if too_many_pages:
                # Слишком много страниц, уменьшаем верхнюю границу вдвое
                new_max = max(square_range['from'] + 10, square_range['to'] // 2)
                new_square_range = {'from': square_range['from'], 'to': new_max}

                self.logger.info(f"Много страниц ({last_page_number}). Уменьшаем до {new_max}")

                yield scrapy.Request(
                    make_url(new_square_range['from'], new_square_range['to']),
                    callback=self.check_range,
                    meta={
                        'base_url': base_url,
                        'square_range': new_square_range,
                        'phase': 'initial',
                    }
                )
            else:
                # Переходим к фазе увеличения
                self.logger.info(f"Переходим к фазе увеличения диапазона")

                # Начинаем с агрессивного увеличения (на 50)
                increase_step = min(50, 999 - square_range['to'])
                if increase_step <= 0:
                    # Достигли максимума, начинаем парсинг
                    yield scrapy.Request(
                        make_url(square_range['from'], square_range['to']),
                        callback=self.parse,
                        meta={
                            'page': 1,
                            'base_url': base_url,
                            'ads_count': 0,
                            'square_range': square_range
                        }
                    )
                else:
                    new_square_range = {'from': square_range['from'], 'to': square_range['to'] + increase_step}

                    yield scrapy.Request(
                        make_url(new_square_range['from'], new_square_range['to']),
                        callback=self.check_range,
                        meta={
                            'base_url': base_url,
                            'square_range': new_square_range,
                            'previous_good': square_range,
                            'phase': 'expand',
                            'step': increase_step,
                        }
                    )

        elif phase == 'expand':
            current_step = response.meta.get('step', 50)

            if too_many_pages:
                # Нашли границу, переходим к бинарному поиску
                # previous_good - последний хороший диапазон
                # square_range - текущий плохой диапазон
                prev_to = previous_good['to']
                current_to = square_range['to']

                # Средняя точка между последним хорошим и текущим плохим
                mid_point = prev_to + (current_to - prev_to) // 2

                if mid_point == prev_to:
                    # Не можем дальше уточнять, используем последний хороший диапазон
                    self.logger.info(f"Найден оптимальный диапазон: {previous_good['from']}-{previous_good['to']} м²")

                    yield scrapy.Request(
                        make_url(previous_good['from'], previous_good['to']),
                        callback=self.parse,
                        meta={
                            'page': 1,
                            'base_url': base_url,
                            'ads_count': 0,
                            'square_range': previous_good
                        }
                    )
                else:
                    # Бинарный поиск
                    new_square_range = {'from': square_range['from'], 'to': mid_point}

                    self.logger.info(f"Бинарный поиск между {prev_to} и {current_to}: пробуем {mid_point}")

                    yield scrapy.Request(
                        make_url(new_square_range['from'], new_square_range['to']),
                        callback=self.check_range,
                        meta={
                            'base_url': base_url,
                            'square_range': new_square_range,
                            'previous_good': previous_good,
                            'bad_range': square_range,
                            'phase': 'binary_search',
                        }
                    )
            else:
                # Можем увеличить еще больше
                if square_range['to'] >= 999:
                    # Достигли максимума
                    self.logger.info(f"Достигли максимального диапазона (999 м²)")

                    yield scrapy.Request(
                        make_url(square_range['from'], square_range['to']),
                        callback=self.parse,
                        meta={
                            'page': 1,
                            'base_url': base_url,
                            'ads_count': 0,
                            'square_range': square_range
                        }
                    )
                else:
                    # Удваиваем шаг для более быстрого поиска границы
                    next_step = min(current_step * 2, 999 - square_range['to'])

                    if next_step <= 0:
                        # Достигли максимума
                        self.logger.info(f"Достигли максимального диапазона (999 м²)")

                        yield scrapy.Request(
                            make_url(square_range['from'], square_range['to']),
                            callback=self.parse,
                            meta={
                                'page': 1,
                                'base_url': base_url,
                                'ads_count': 0,
                                'square_range': square_range
                            }
                        )
                    else:
                        new_square_range = {'from': square_range['from'], 'to': square_range['to'] + next_step}

                        self.logger.info(f"Увеличиваем до {new_square_range['to']} (шаг: {next_step})")

                        yield scrapy.Request(
                            make_url(new_square_range['from'], new_square_range['to']),
                            callback=self.check_range,
                            meta={
                                'base_url': base_url,
                                'square_range': new_square_range,
                                'previous_good': square_range,
                                'phase': 'expand',
                                'step': next_step,
                            }
                        )

        elif phase == 'binary_search':
            bad_range = response.meta.get('bad_range')

            if too_many_pages:
                # Текущая точка тоже "плохая"
                new_bad = square_range
                # Средняя точка между последним хорошим и новым плохим
                mid_point = previous_good['to'] + (new_bad['to'] - previous_good['to']) // 2

                if mid_point == previous_good['to']:
                    # Не можем дальше уточнять, используем последний хороший диапазон
                    self.logger.info(f"Найден оптимальный диапазон: {previous_good['from']}-{previous_good['to']} м²")

                    yield scrapy.Request(
                        make_url(previous_good['from'], previous_good['to']),
                        callback=self.parse,
                        meta={
                            'page': 1,
                            'base_url': base_url,
                            'ads_count': 0,
                            'square_range': previous_good
                        }
                    )
                else:
                    # Продолжаем бинарный поиск
                    new_square_range = {'from': square_range['from'], 'to': mid_point}

                    self.logger.info(f"Бинарный поиск: пробуем {mid_point}")

                    yield scrapy.Request(
                        make_url(new_square_range['from'], new_square_range['to']),
                        callback=self.check_range,
                        meta={
                            'base_url': base_url,
                            'square_range': new_square_range,
                            'previous_good': previous_good,
                            'bad_range': new_bad,
                            'phase': 'binary_search',
                        }
                    )
            else:
                # Текущая точка "хорошая"
                if square_range['to'] + 1 >= bad_range['to']:
                    # Нашли оптимальную точку
                    self.logger.info(f"Найден оптимальный диапазон: {square_range['from']}-{square_range['to']} м²")

                    yield scrapy.Request(
                        make_url(square_range['from'], square_range['to']),
                        callback=self.parse,
                        meta={
                            'page': 1,
                            'base_url': base_url,
                            'ads_count': 0,
                            'square_range': square_range
                        }
                    )
                else:
                    # Продолжаем поиск
                    mid_point = square_range['to'] + (bad_range['to'] - square_range['to']) // 2
                    new_square_range = {'from': square_range['from'], 'to': mid_point}

                    self.logger.info(f"Бинарный поиск: пробуем {mid_point}")

                    yield scrapy.Request(
                        make_url(new_square_range['from'], new_square_range['to']),
                        callback=self.check_range,
                        meta={
                            'base_url': base_url,
                            'square_range': new_square_range,
                            'previous_good': square_range,
                            'bad_range': bad_range,
                            'phase': 'binary_search',
                        }
                    )

    def parse(self, response):
        current_page = response.meta.get('page', 1)
        base_url = response.meta.get('base_url', response.url)
        ads_count = response.meta.get('ads_count', 0)
        square_range = response.meta.get('square_range', {'from': self.min_square, 'to': self.max_square})
        first_check = response.meta.get('first_check', False)

        ads = []

        self.logger.info(f"Парсинг страницы {current_page} для {base_url}")
        self.logger.info(f"Текущий диапазон площади: от {square_range['from']} до {square_range['to']} м²")

        # Находим все элементы с классом grid-search-content
        grid_items = response.css('.grid-search-content')

        # ВАЖНО: Проверяем наличие элемента пагинации с rel="last" и href, содержащим page=30
        last_page_link = response.css('a[rel="last"]')
        too_many_pages = False

        if last_page_link:
            href = last_page_link.css('::attr(href)').get()
            if href and 'page=30' in href:
                too_many_pages = True
                self.logger.info("Обнаружена пагинация с последней страницей 30 или более")

        # Если это первая проверка и обнаружено слишком много страниц, уменьшаем верхний предел площади
        if first_check and too_many_pages:
            self.logger.info(f"Обнаружено слишком много страниц. Уменьшаем верхний предел площади.")

            # Уменьшаем верхний предел площади
            new_max = square_range['to'] - self.reduction_step
            if new_max <= square_range['from']:
                new_max = square_range['from'] + 10  # Минимальный диапазон

            new_square_range = {'from': square_range['from'], 'to': new_max}

            # Формируем URL с новыми параметрами площади
            if '?' in base_url:
                next_url = f"{base_url}&total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"
            else:
                next_url = f"{base_url}?total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"

            self.logger.info(
                f"Пробуем с новым диапазоном: от {new_square_range['from']} до {new_square_range['to']} м²")
            yield scrapy.Request(
                next_url,
                callback=self.parse,
                meta={
                    'page': 1,
                    'base_url': base_url,
                    'ads_count': 0,
                    'square_range': new_square_range,
                    'first_check': True  # Продолжаем проверять
                }
            )
            return

        # Если на странице нет объявлений, переходим к следующему диапазону
        if not grid_items and first_check:
            self.logger.info(f"В диапазоне {square_range['from']}-{square_range['to']} м² не найдено объявлений.")

            # Если текущий диапазон не весь (не до 999), переходим к следующему
            if square_range['to'] < 999:
                new_from = square_range['to'] + 1
                new_to = 999

                new_square_range = {'from': new_from, 'to': new_to}

                # Формируем URL с новыми параметрами площади
                if '?' in base_url:
                    next_url = f"{base_url}&total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"
                else:
                    next_url = f"{base_url}?total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"

                self.logger.info(
                    f"Переходим к следующему диапазону: от {new_square_range['from']} до {new_square_range['to']} м²")
                yield scrapy.Request(
                    next_url,
                    callback=self.parse,
                    meta={
                        'page': 1,
                        'base_url': base_url,
                        'ads_count': 0,
                        'square_range': new_square_range,
                        'first_check': True
                    }
                )
            else:
                self.logger.info(f"Достигнут конец диапазона площадей. Завершаем парсинг.")
            return

        # Если на странице нет объявлений при пагинации, прекращаем пагинацию
        if not grid_items and not first_check:
            self.logger.info(f"На странице {current_page} для {base_url} не найдено объявлений. Прекращаем пагинацию.")

            # Переходим к следующему диапазону
            new_from = square_range['to'] + 1
            new_to = 999

            new_square_range = {'from': new_from, 'to': new_to}

            # Формируем URL с новыми параметрами площади
            if '?' in base_url:
                next_url = f"{base_url}&total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"
            else:
                next_url = f"{base_url}?total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"

            self.logger.info(
                f"Переходим к следующему диапазону: от {new_square_range['from']} до {new_square_range['to']} м²")
            yield scrapy.Request(
                next_url,
                callback=self.parse,
                meta={
                    'page': 1,
                    'base_url': base_url,
                    'ads_count': 0,
                    'square_range': new_square_range,
                    'first_check': True
                }
            )
            return

        self.logger.info(f"Найдено {len(grid_items)} элементов grid-search-content")

        # Счетчик объявлений на текущей странице
        page_ads_count = 0

        # Формируем базовое имя файла для сохранения
        url_part = base_url.split('/')[-1].split('?')[0]
        base_filename = f"results/ads_{url_part}_sq_{square_range['from']}-{square_range['to']}"

        # Обрабатываем найденные объявления
        for item in grid_items:
            # Находим ссылки с классом title-adv
            title_links = item.css('.title-adv')
            for link in title_links:
                href = link.css('::attr(href)').get()
                title = link.css('::text').get()

                ad_data = {
                    'title': title.strip() if title else '',
                    'url': response.urljoin(href),
                }
                ads.append(ad_data)
                # Сохраняем каждое объявление отдельно
                self.logger.info(f"Найдено объявление: {title.strip() if title else ''} - {href}")

                # **Формируем путь к JSON-файлу**
                filename = f"results/ads_{square_range['from']}-{square_range['to']}.json"

                # **Если файл уже существует, загружаем старые данные**
                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8') as f:
                        try:
                            existing_ads = json.load(f)
                        except json.JSONDecodeError:
                            existing_ads = []
                else:
                    existing_ads = []

                # **Добавляем новые объявления и записываем обратно**
                existing_ads.extend(ads)

                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(existing_ads, f, ensure_ascii=False, indent=4)

                self.logger.info(f"Обновлен файл {filename}, всего объявлений: {len(existing_ads)}")

        # Если нашли объявления и не достигли предела пагинации, переходим к следующей странице
        if page_ads_count > 0 and current_page < 30 and not too_many_pages:
            next_page = current_page + 1

            # Формируем URL следующей страницы с сохранением параметров площади
            if '?' in base_url:
                next_url = f"{base_url}&total_square_from={square_range['from']}&total_square_to={square_range['to']}&page={next_page}"
            else:
                next_url = f"{base_url}?total_square_from={square_range['from']}&total_square_to={square_range['to']}&page={next_page}"

            self.logger.info(f"Переход к странице {next_page}: {next_url}")
            yield scrapy.Request(
                next_url,
                callback=self.parse,
                meta={
                    'page': next_page,
                    'base_url': base_url,
                    'ads_count': ads_count,
                    'square_range': square_range,
                    'first_check': False
                }
            )
        else:
            # Если дошли до конца пагинации или нет объявлений, переходим к следующему диапазону
            if current_page >= 30 or too_many_pages:
                self.logger.info(
                    f"Достигнут предел пагинации для диапазона {square_range['from']}-{square_range['to']} м².")

            # Переходим к следующему диапазону
            new_from = square_range['to'] + 1
            new_to = 999

            if new_from < 999:
                new_square_range = {'from': new_from, 'to': new_to}

                # Формируем URL с новыми параметрами площади
                if '?' in base_url:
                    next_url = f"{base_url}&total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"
                else:
                    next_url = f"{base_url}?total_square_from={new_square_range['from']}&total_square_to={new_square_range['to']}"

                self.logger.info(
                    f"Переходим к следующему диапазону: от {new_square_range['from']} до {new_square_range['to']} м²")
                yield scrapy.Request(
                    next_url,
                    callback=self.parse,
                    meta={
                        'page': 1,
                        'base_url': base_url,
                        'ads_count': 0,
                        'square_range': new_square_range,
                        'first_check': True
                    }
                )
            else:
                self.logger.info(f"Достигнут конец диапазона площадей. Завершаем парсинг.")
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
    process.crawl(SobAdsSpider)   # Для сбора объявлений
    # process.crawl(SobDetailSpider)  # Для сбора деталей объявлений
    process.start()