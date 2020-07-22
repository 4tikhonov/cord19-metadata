# CORD-19 Common Preprocessing Module
This module was created to get CORD-19 papers metadata synchronized in MongoDB and Elasticsearch and converted to various bibliographic standards (MARC21, etc).

## CORD-19 collection
* Download the original collection from [Kaggle](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge)
* unzip archive in some folder on your hard drive, for example, /corddata
* edit api/config.py and change "maindir" to your folder, "cordversion" to reflect the current CORD-19 version from Kaggle (v38 at the moment)

## Setup Mongo locally and create a user
```
mongo admin
db.createUser({user: "coronawhyguest" , pwd: "coro901na", roles: [  "readWriteAnyDatabase" ]});

```
## Run ingest process to get CORD-19 metadata in Mongo 
```
python3 ./start.py
``` 
### Check CORD-19 metadata
Login to Mongo and check imported CORD-19 metadata records
```
mongo -u coronawhyguest -p coro901na cord19
db.v38.find().count()

```

