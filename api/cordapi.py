from config import mongouser, mongopass
from pymongo import MongoClient
from elasticsearch import Elasticsearch

client = MongoClient("mongodb://%s:%s@mongodb.coronawhy.org" % (mongouser, mongopass))
db = client.get_database('cord19')
collection = db.v19

x = collection.find({'cord_uid': '7ots8npg'})
for item in x:
    print(item)



