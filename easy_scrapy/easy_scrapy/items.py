# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EasyScrapyItem(scrapy.Item):
    record = scrapy.Field()     # container for fields in topic
    general = scrapy.Field()  # container for fields in file
    pass

class GeneralCrawlerItem(scrapy.Item):
    record = scrapy.Field()     # container for fields in topic
    general = scrapy.Field()  # container for fields in file
    pass