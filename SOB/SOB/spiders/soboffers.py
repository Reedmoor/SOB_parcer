from typing import Any, Generator, Iterable
import scrapy
import json
import re
import os
from scrapy.http import Request, Response
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from datetime import datetime
from urllib.parse import urljoin

class SobOffersSpider(scrapy.Spider):
    name = "soboffers"
    version = "2025.02.20"
    allowed_domains = ["sob.ru"]
    start_urls = ["https://sob.ru/"]

    def __init__(self, name: str | None = None, **kwargs: Any) -> None:
        self.logger.info("Starting SobOffers %s", self.version)
        super().__init__(name, **kwargs)

    def start_requests(self) -> Iterable[Request]:
        self.logger.info("Starting parsing from main page")
        yield Request(
            url=self.start_urls[0],
            method="GET",
            callback=self.parse_menu,
            errback=self.handle_error
        )

    def parse_menu(self, response: Response) -> Iterable[Request]:
        self.logger.info("Parsing site menu")

        header_menu = response.xpath('//ul[contains(@class, "header-menu-list")]')
        if not header_menu:
            self.logger.error("Menu not found!")
            return None

        menu_items = header_menu.xpath('./li')
        results = {}

        for item in menu_items:
            menu_text = item.xpath('./a/text()').get()
            if not menu_text or menu_text.strip() not in ['Продажа', 'Аренда']:
                continue

            menu_text = menu_text.strip()
            results[menu_text] = []
            
            links = item.xpath('.//a[@href]')
            for link in links:
                url = link.xpath('./@href').get()
                title = link.xpath('./text()').get()
                if not title:
                    continue

                title = title.strip()
                parent_ul = link.xpath('./parent::li/parent::ul')
                region_text = parent_ul.xpath('./li[1]/text()').get()
                region = region_text.strip() if region_text else "Unknown"

                link_data = {
                    'title': title,
                    'url': response.urljoin(url),
                    'region': region
                }
                results[menu_text].append(link_data)

        # Start parsing categories
        for category in results:
            for item in results[category]:
                yield Request(
                    url=item['url'],
                    method="GET",
                    callback=self.parse_category,
                    errback=self.handle_error,
                    meta={
                        'category': category,
                        'subcategory': item['title'],
                        'region': item['region'],
                        'page': 1
                    }
                )

    def parse_category(self, response: Response) -> Iterable[Request]:
        current_page = response.meta.get('page', 1)
        category = response.meta.get('category')
        subcategory = response.meta.get('subcategory')
        region = response.meta.get('region')
        base_url = response.meta.get('base_url', response.url)
        ads_count = response.meta.get('ads_count', 0)
        square_from = response.meta.get('square_from')
        square_to = response.meta.get('square_to')

        self.logger.info(f"Parsing category {category}/{subcategory} ({region}) - page {current_page}")

        last_page_link = response.xpath('//a[@rel="last"]/b/text()').get()

        if current_page == 1 and last_page_link and int(last_page_link) >= 30:
            yield self.create_area_filtered_request(base_url, square_from, square_to, ads_count)
            return

        grid_items = response.xpath('//*[contains(@class, "grid-search-content")]')
        if not grid_items:
            return

        for item in grid_items:
            title_links = item.xpath('.//*[contains(@class, "title-adv")]')
            for link in title_links:
                href = link.xpath('./@href').get()
                title = link.xpath('./text()').get()

                if href and title:
                    yield Request(
                        url=response.urljoin(href),
                        method="GET",
                        callback=self.parse_offer,
                        errback=self.handle_error,
                        meta={
                            'category': category,
                            'subcategory': subcategory,
                            'region': region
                        }
                    )

        if last_page_link and current_page < int(last_page_link):
            next_page = current_page + 1
            next_url = self._create_pagination_url(base_url, next_page, square_from, square_to)
            
            yield Request(
                next_url,
                method="GET",
                callback=self.parse_category,
                errback=self.handle_error,
                meta={
                    'page': next_page,
                    'base_url': base_url,
                    'ads_count': ads_count,
                    'square_from': square_from,
                    'square_to': square_to,
                    'category': category,
                    'subcategory': subcategory,
                    'region': region
                }
            )

    def parse_offer(self, response: Response) -> Iterable[dict]:
        self.logger.info(f"Parsing offer details: {response.url}")

        agent_info = response.xpath('//div[@class="b-card-info-default__2column__agent"]/text()').getall()
        agent_info = ' '.join([text.strip() for text in agent_info if text.strip()])
        is_agency = 1 if 'Агентство' in agent_info or 'Компания' in agent_info else 0

        rent_term = response.xpath('//div[@class="flex-four-equals"]//p[contains(text(), "Срок аренды")]/text()').get()
        deal_type = 3 if rent_term else 2

        image_urls = response.xpath("//div[@class='adv-page-photos']//a/@href").getall()
        image_urls = ["https" + url if not url.startswith("http") else url for url in image_urls]

        categories = [
            {
                "href": urljoin(response.url, a.xpath("./@href").get()),
                "title": a.xpath("./span/text()").get("").strip(),
            } for a in response.xpath("//ul[@class='breadcrumbs']//a")
        ]

        attributes = {}
        for block in response.xpath('//div[@class="adv-page-content2"]//div[@class="flex-four-equals"]/div'):
            for attr in block.xpath('./p'):
                attr_text = attr.get()
                if attr_text:
                    attr_name_match = re.search(r'<b>([^<]+)</b>', attr_text)
                    if attr_name_match:
                        attr_name = attr_name_match.group(1).strip().rstrip(':')
                        attr_value = re.sub(r'<[^>]+>', '', attr_text.split('</b>', 1)[1])
                        attributes[attr_name] = attr_value.strip()

        date_text = response.xpath('//div[@class="flex-two-equals"]'
                                 '//p[@class="text-date"][contains(text(), "Дата публикации")]/text()').get()

        publication_date = None
        if date_text:
            date_match = re.search(r'Дата публикации: (\d{1,2}) (\w+) (\d{4})', date_text)
            if date_match:
                day = date_match.group(1).zfill(2)
                month_name = date_match.group(2).lower()
                year = date_match.group(3)

                ru_month_map = {
                    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                    'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                    'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
                }

                if month_name in ru_month_map:
                    month = ru_month_map[month_name]
                    publication_date = f"{year}-{month}-{day}"

        offer = {
            'url': response.url,
            'title': response.xpath('//div[@class="adv-page-title"]/h1/text()').get("").strip(),
            'price': re.sub(r'[^\d]', '', response.xpath('//p[@class="text-price"]/text()').get("").strip()),
            'phone': re.sub(r'[^\d]', '', response.xpath('//div[@class="phone-show-visible"]/p/b/text()').get("")),
            'is_agency': is_agency,
            'region': response.meta.get('region', 'Unknown'),
            'city': response.xpath('//div[@class="flex-two-equals"]//a[@class="black-link"]/text()').get("").replace(' г.', '').replace('пос.', '').strip() or "Moscow",
            'address': ' '.join(response.xpath('//div[@class="adv-page-content1"]//p//a[@class="black-link"]/text()').getall()) or response.xpath('//div[@class="adv-page-content1"]//p[contains(text(), "Адрес")]/text()').get("").strip(),
            'description': ' '.join(p.strip() for p in response.xpath('//div[@class="adv-page-desc"]//p/text()').getall() if p.strip()),
            'deal_type': deal_type,
            'images': image_urls,
            'categories': categories,
            'attributes': attributes,
            'publication_date': publication_date
        }

        yield offer

    def create_area_filtered_request(self, base_url, square_from, square_to, ads_count):
        if square_from is None and square_to is None:
            square_from = 1
            square_to = 999
        elif square_to > square_from:
            new_square_to = square_to // 2
            if new_square_to <= square_from:
                new_square_to = square_from + 1
            square_to = new_square_to

        return self._create_request(base_url, 1, ads_count, square_from, square_to)

    def _create_request(self, base_url, page, ads_count, square_from, square_to):
        next_url = f"{base_url}{'&' if '?' in base_url else '?'}total_square_from={square_from}&total_square_to={square_to}"

        return Request(
            next_url,
            method='GET',
            callback=self.parse_category,
            errback=self.handle_error,
            meta={
                'page': page,
                'base_url': base_url,
                'ads_count': ads_count,
                'square_from': square_from,
                'square_to': square_to
            }
        )

    def _create_pagination_url(self, base_url, page, square_from, square_to):
        params = []
        if square_from is not None and square_to is not None:
            params.extend([f"total_square_from={square_from}", f"total_square_to={square_to}"])
        params.append(f"page={page}")
        
        separator = '&' if '?' in base_url else '?'
        return f"{base_url}{separator}{'&'.join(params)}"

    def handle_error(self, failure) -> None:
        if failure.check(HttpError):
            self.logger.error(
                "HttpError on %s. Status code: %s",
                failure.value.response.url,
                failure.value.response.status,
            )
        elif failure.check(DNSLookupError):
            self.logger.error("DNSLookupError on %s", failure.request.url)
        elif failure.check((TimeoutError, TCPTimedOutError)):
            self.logger.error("TimeoutError on %s", failure.request.url) 