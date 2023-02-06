import psycopg2
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import sqlite3


class PriceToUSDPipeline:
    gbpToUsdRate = 1.23

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('price'):
            floatPrice = float(adapter['price'])

            adapter['price'] = floatPrice * self.gbpToUsdRate
            return item
        else:
            raise DropItem(f"Missing price in {item}")


class DuplicatesPipeline:
    def __init__(self):
        self.names_seen = set()
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter['name'] in self.names_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.names_seen.add(adapter['name'])
            return item


class SavingToPostgresPipeline(object):
    def __init__(self):
        self.connection = psycopg2.connect(
            host= "localhost",
            dbname= "chocolate_scraping",
            user= "scrapy",
            password= "scrapy",
            port= 5432
        )
        self.curr = self.connection.cursor()
        self.create_table()
    def create_table(self):
        self.curr.execute("""CREATE TABLE IF NOT EXISTS chocolate_products(
                                id SERIAL PRIMARY KEY,
                                name VARCHAR(255),
                                price VARCHAR(255),
                                url TEXT
                                )""")

    def process_item(self, item, spider):
        self.curr.execute("""INSERT INTO chocolate_products(name, price, url) VALUES (%s,%s,%s)""", (
            item["name"],
            item["price"],
            item["url"]
        ))
        self.connection.commit()
        return item


    def close_spider(self, spider):
        self.curr.close()
        self.connection.close()