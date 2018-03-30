import pymongo
from scrapy import log
from datetime import datetime
from scrapy.exceptions import DropItem
#from scrapy import signals


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

        def close_spider(self,spider):
             self.client.close()

#        def get_latest_post(self,spider):
 #           coll=spider.brand+"_"+spider.model
  #          doc_count=self.db[coll].count()
   #         default_date=datetime.strptime("01.01.1900 00:00", "%d.%m.%Y %H:%M")
    #        latest_post=self.db[coll].find_one({},{'Timestamp':True, '_id':False}, sort=[('Timestamp', -1)])['Timestamp'] if doc_count>0 else default_date
     #       #spider.checked_timestamp=True
      #      return latest_post

        def process_item(self, item, spider):
             coll=spider.brand+"_"+spider.model
             if (coll in self.db.collection_names()):
                 #if not spider.checked_timestamp:
                 in_db=self.db[coll].find({'Timestamp':item['Timestamp']}).count()
                 if in_db>0:
                     log.msg("post already in db", level=log.INFO)
                     # TODO end spider
                     raise DropItem("already in DB")
                 else:
                     print("send out notification for new post "+str(item['Timestamp']))
                     self.db[coll].insert(item)
                     # TODO Notification to user
                     log.msg('Item added', level=log.INFO)
                     return item


