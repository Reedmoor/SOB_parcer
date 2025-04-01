# SOB

Сбор данных с объявлений в sob.ru
результат будет лежать в sob_details.json

## Расположение

- **Сервер**: IP сервера / Домен
- **Путь к проекту**: /.../.../.../parser

## Запуск

- Локально
    1. Клонировать репозиторий `git clone https://github.com/Reedmoor/SOB_parcer`
    2. Установить зависимости `pip install scrapy`
    3. `scrapy crawl soboffers`

## Конфигурация

Конфиг указывается в файле `.env` (Или в любом другом)

```env
CATEGORY = "frezery"
```

## Модели/Структуры данных

### Детали объявления

```json
{
        "url": "",
        "title": "",
        "price": "",
        "phone": "",
        "is_agency": ,
        "region": "",
        "city": "",
        "address": "",
        "description": "",
        "deal_type": ,
        "images": [],
        "categories": [],
        "attributes": {},
        "publication_date": ""
    }
```

## Основные зависимости

| Название      | Ссылка                                            |
| ------------- | ------------------------------------------------- |
| Scrapy        | [Ссылка](https://pypi.org/project/Scrapy/)        |
| lxml          | [Ссылка](https://pypi.org/project/lxml/)          |
| python-dotenv | [Ссылка](https://pypi.org/project/python-dotenv/) | 
