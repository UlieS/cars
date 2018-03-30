import json
import scrapy
from time import strftime
from datetime import datetime
import dateparser

class CarSpider(scrapy.Spider):
    name = "cars"

    def start_requests(self):
        with open('brand_codes.json','r') as brand_file:
             brand_codes=json.loads(brand_file.read())

        brand_code=brand_codes[self.brand]

        urls = [
             'https://suchen.mobile.de/fahrzeuge/search.html?damageUnrepaired=NO_DAMAGE_UNREPAIRED&grossPrice=true&isSearchRequest=true&lang=en&makeModelVariant1.makeId='+brand_code+'&makeModelVariant1.modelGroupId=40&maxMileage='+self.mileage+'&maxPrice='+self.price+'&scopeId=C&sortOption.sortBy=creationTime&sortOption.sortOrder=DESCENDING',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        next_page=response.xpath("//span[contains(@class, 'page-forward')]/@data-href").extract()

        listed_cars=[]
        timestamp=[]
        for el in response.xpath("//div[contains(@class, 'resultitem')]/a"):
            listed_cars.append(el.xpath("@href").extract()[0])
            t=el.xpath(".//span[contains(@class, 'onlineSince')]/text()").extract()[0].split(" ", 3)[-1]
            timestamp.append(dateparser.parse(t))
        tuple_list=zip(listed_cars,timestamp)

        for car_url,time in tuple_list:
            yield scrapy.Request(url=car_url,callback=lambda r, t=time:self.parse_entries(r,t))

        if next_page:
            yield scrapy.Request(url=next_page[0], callback=self.parse)


    def parse_entries(self, response, timestamp):

        car=dict()
        car['Title']=response.xpath("//div[contains(@class, 'title')]/div/div/h1/text()").extract()[0]
        car['Brand']=self.brand
        car['Model']=self.model
        car['Timestamp']=timestamp

        for node in response.xpath("//div[contains(@class, 'technical-data')]/div"):
            key=node.xpath("div/strong/text()").extract()[0]
            val=node.xpath("div/text()").extract()
            if not val:
                price=node.xpath("div/span/text()").extract()
                if price:
                    car['Price']=price[0]
            else:
                value=val[0] if len(val)==1 else val[1]
                car[key]=value

        features=[]
        for node in response.xpath("//div[contains(@id, 'features')]/div"):
            features=node.xpath("div/div/p/text()").extract()

        car['Feature']=features
        return car
