# -*- coding: utf-8 -*-
from config import altmetricsID, mongouser, mongopass
import json
import codecs

fileURL = "http://datasets.coronawhy.org/api/access/datafile/%s?format=original&gbrecs=true" % altmetricsID
filename = '103.x.json'
#data = json.load(codecs.open(filename, 'r', 'utf-8-sig'))

with open(filename) as json_file:
    data = json.load(json_file)
    #print(data)
    for p in data:
        print(p)
