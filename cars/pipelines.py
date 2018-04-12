import pymongo
import json
from scrapy import log
from datetime import datetime
from scrapy.exceptions import DropItem
#from bson import json_util

class CarsPipeline(object):

        def __init__(self,mongo_uri, mongo_db):
             self.mongo_uri=mongo_uri
             self.mongo_db =mongo_db

        @classmethod
        def from_crawler(cls, crawler):
             return cls(
                 mongo_uri=crawler.settings.get('MONGO_URI'),
                 mongo_db=crawler.settings.get('MONGO_DB'),
             )

        def open_spider(self,spider):
             self.client=pymongo.MongoClient(self.mongo_uri)
             self.db=self.client[self.mongo_db]
#            self.file=open('items.jl', 'w')
             self.file=open('items.txt', 'w')

        def close_spider(self,spider):
             self.client.close()
             self.file.close()

        def format(self, item):
             line="\n\n" 
             for k in item.keys():
                 if k=="_id": continue 
                 value=str(item[k]) if not isinstance(item[k], datetime) else item[k].strftime("%d.%m.%Y %H:%M")
                 line=line + str(k)+": "+ value +"\n"
             return line

        def process_item(self, item, spider):
             coll=spider.brand+"_"+spider.model
             if (coll in self.db.collection_names()):
                 in_db=self.db[coll].find({'Timestamp':item['Timestamp'], 'Title': item['Title'], 'Price':item['Price']}).count()

                 if in_db>0:
                     print("already in db")
                     raise DropItem("already in DB")

                 else:
                     print("send out notification for new post "+str(item['Timestamp']))
                     self.db[coll].insert(item)
                     # TODO Notification to user
                     # line = json.dumps(dict(item), default=json_util.default)+"\n"
                     self.file.write(self.format(item))
                     log.msg('Item added', level=log.INFO)
                     return item
             else:
                 self.db.createCollection(coll)
                 self.db[coll].insert(item)
                 log.msg('Collection ' + coll + ' added', level=log.INFO)
                 return item
 
