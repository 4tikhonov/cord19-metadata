# cord19
CORD-19 common module

## CORD-19 collection
* Download the original collection from (Kaggle)[https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge]
* unzip archive in some folder on your hard drive, for example, /corddata
* edit api/config.py and change maindir to your folder

## Setup Mongo locally and create a user
```
mongo admin
db.createUser({user: "coronawhyguest" , pwd: "coro901na", roles: [  "readWriteAnyDatabase" ]});

```
## Run ingest process to get CORD-19 metadata in Mongo 
```
python3 ./start.py
``` 

