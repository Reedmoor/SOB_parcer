import os
from datetime import datetime

BOT_NAME = "sob"

SPIDER_MODULES = ["SOB.spiders"]
NEWSPIDER_MODULE = "SOB.spiders"


LOG_LEVEL = "INFO"
os.makedirs("logs", exist_ok=True)
LOG_FILE = f"./logs/{datetime.now().strftime('%Y.%m.%d')}.log"

# User Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 YaBrowser/24.4.0.0 Safari/537.36"

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 32
CONCURRENT_REQUESTS_PER_DOMAIN = 32

DOWNLOAD_DELAY = 0.25 / 32


ITEM_PIPELINES = {}
DOWNLOADER_MIDDLEWARES = {}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
FEEDS = {
     f"./data/offers_{datetime.now().strftime('%Y.%m.%d')}.json": {
        'format': 'json',
        'encoding': 'utf8',
        'store_empty': False,
        'fields': None,
        "overwrite": True,
        },
    }

