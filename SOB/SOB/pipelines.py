# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class DuplicateFilterPipeline:
    def __init__(self):
        self.urls_seen = set()

    def open_spider(self, spider):
        # Load existing URLs from the output file if it exists
        import os
        import json
        output_path = '../../../results/sob_details.json'
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    self.urls_seen = {item['url'] for item in data if 'url' in item}
                except:
                    pass

    def process_item(self, item, spider):
        if item['url'] in self.urls_seen:
            spider.logger.info(f"Duplicate item found: {item['url']}")
            raise DropItem(f"Duplicate item found: {item['url']}")
        self.urls_seen.add(item['url'])
        return item