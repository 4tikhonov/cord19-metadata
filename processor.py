import pandas as pd 
import re
import os
import numpy as np
from commons import CORDCommons
import json
from api.config import mongouser, mongopass, mongohost, doindex, cordversion
from pymongo import MongoClient
from elasticsearch import Elasticsearch

class CORDProcessor():
    def __init__(self, df, directory):    
        self.df = df
        self.directory = directory
        self.mongoclient = MongoClient("mongodb://%s:%s@%s" % (mongouser, mongopass, mongohost))
        self.db = self.mongoclient.get_database('cord19')
        self.collection = self.db[cordversion]
        
    def has_v15_metadata(self):
        if self.directory:
            jsfile = {}
            self.papers = {}
            for i in self.df.index:
                self.metadata = {}
                self.rows = []
                doi = self.df.iloc[i].doi
                relpath = self.df.iloc[i].pdf_json_files
                section = (str(relpath) + "/") * 2
                pmcid = self.df.iloc[i].pmcid
                filename = self.directory + '/' + relpath
                print("%s\n" % filename)
                self.metadata['path'] = relpath
                self.metadata['doi'] = doi
                try:
                    with open(filename) as paperjs:
                        jsfile = json.load(paperjs)
                except:
                    print("Problem with", self.df.iloc[i].cord_uid)
                    #continue

                _id = self.df.iloc[i]["cord_uid"]
                self.metadata['cord_uid'] = _id
                #print(filename)
                #print(jsfile)
        
                if "title" in jsfile.keys():
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                    self.metadata['title'] = jsfile["title"]
                else:
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=self.df.iloc[i].title))
                    self.metadata['title'] = self.df.iloc[i].title
        
                if "abstract" in jsfile.keys():
                    if len(jsfile["abstract"]) > 1:
                        for j in range(len(jsfile["abstract"])):
                            self.metadata['abstract'] = jsfile["abstract"]
                            self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"][j]["text"]))
                    else:
                        self.metadata['abstract'] = jsfile["abstract"]
                        self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"]))
                elif "abstract" in jsfile["metadata"].keys():
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["metadata"]["abstract"]))
                    self.metadata['abstract'] = jsfile["metadata"]["abstract"]
                else:
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=self.df.iloc[i].abstract))
                    self.metadata['abstract'] = self.df.iloc[i].abstract

                sections = list(set([k["section"] for k in jsfile["body_text"]]))

                for section in sections:
                    self.metadata['body_text'] = jsfile["body_text"]
                    for l in range(len(jsfile["body_text"])):
                        if jsfile["body_text"][l]["section"] == section:
                            if section == '':
                                section = "body_text"
                            self.rows.append(dict(cord_uid=_id, section=section,
                                     subsection=l, text=jsfile["body_text"][l]["text"]))

                c = CORDCommons(self.directory)
                tables = c.extract_tables_from_json(jsfile)
                self.metadata['tables'] = tables
                for table in tables:
                    self.rows.append(dict(cord_uid=_id, section=table[0], subsection=table[1], text=table[2]))
                self.metadata['body_rows'] = self.rows
                if doindex == 'mongo':
                    try:
                        self.collection.insert_one(self.metadata)
                    except: 
                        print("errors with doi %s" % doi)
                else:
                    self.papers[doi] = self.metadata
        return

    def has_pmc_xml_parse(self):
        if self.directory:
            jsfile = {}
            self.papers = {}
            for i in self.df[self.df["has_pmc_xml_parse"] == 1].index:
                self.metadata = {}
                self.rows = []
                doi = self.df.iloc[i].doi
                section = (str(self.df.iloc[i].full_text_file) + "/") * 2
                pmcid = self.df.iloc[i].pmcid
                filename = self.directory + '/' + section + "pmc_json/" + pmcid + ".xml.json"
                self.metadata['path'] = section + "pmc_json/" + pmcid + ".xml.json"
                print(filename)
                try:
                    with open(filename) as paperjs:
                        jsfile = json.load(paperjs)
                except:
                    print("Problem with", self.df.iloc[i].cord_uid)
                    #continue

                _id = self.df.iloc[i]["cord_uid"]
                self.metadata['cord_uid'] = _id
                #print(filename)
                #print(jsfile)
        
                if "title" in jsfile.keys():
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                    self.metadata['title'] = jsfile["title"]
                else:
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=self.df.iloc[i].title))
                    self.metadata['title'] = self.df.iloc[i].title
        
                if "abstract" in jsfile.keys():
                    if len(jsfile["abstract"]) > 1:
                        for j in range(len(jsfile["abstract"])):
                            self.metadata['abstract'] = jsfile["abstract"]
                            self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"][j]["text"]))
                    else:
                        self.metadata['abstract'] = jsfile["abstract"]
                        self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"]))
                elif "abstract" in jsfile["metadata"].keys():
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["metadata"]["abstract"]))
                    self.metadata['abstract'] = jsfile["metadata"]["abstract"]
                else:
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=self.df.iloc[i].abstract))
                    self.metadata['abstract'] = self.df.iloc[i].abstract

                sections = list(set([k["section"] for k in jsfile["body_text"]]))

                for section in sections:
                    self.metadata['body_text'] = jsfile["body_text"]
                    for l in range(len(jsfile["body_text"])):
                        if jsfile["body_text"][l]["section"] == section:
                            if section == '':
                                section = "body_text"
                            self.rows.append(dict(cord_uid=_id, section=section,
                                     subsection=l, text=jsfile["body_text"][l]["text"]))

                c = CORDCommons(self.directory)
                tables = c.extract_tables_from_json(jsfile)
                self.metadata['tables'] = tables
                for table in tables:
                    self.rows.append(dict(cord_uid=_id, section=table[0], subsection=table[1], text=table[2]))
                self.metadata['body_rows'] = self.rows
                self.papers[doi] = self.metadata
        return
    
    def has_no_pmc_xml_parse(self):
        if self.directory:
            self.papers = {}
            jsfile = {}
            self.df = self.df[self.df["has_pdf_parse"] == 1]
            self.df = self.df[self.df["has_pmc_xml_parse"] == 0]
            self.df = self.df[0:10]
            print(self.df.index)
            for i in self.df[(self.df["has_pmc_xml_parse"] == 0) & (self.df["has_pdf_parse"] == 1)].index:
                self.metadata = {}
                self.rows = []
                section = (str(self.df.iloc[i].full_text_file) + "/") * 2
                doi = self.df.iloc[i].doi
                sha = self.df.iloc[i].sha
                if len(sha.split("; ")) > 1:
                    sha = sha.split("; ")[0]
                filename = self.directory + '/' + section + "pdf_json/" + sha + ".json"
                self.metadata['path'] = section + "pdf_json/" + sha + ".json"
                try:
                    with open(filename) as paperjs:
                        jsfile = json.load(paperjs)
                except:
                    print("Problem with", self.df.iloc[i].cord_uid)

                _id = self.df.iloc[i]["cord_uid"]
                self.metadata['cord_uid'] = _id
                if "title" in jsfile.keys():
                    self.metadata['title'] = jsfile["title"]
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                else:
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=self.df.iloc[i].title))
                    self.metadata['title'] = self.df.iloc[i].title
                    
                if "abstract" in jsfile.keys():
                    if len(jsfile["abstract"]) > 1:
                        for j in range(len(jsfile["abstract"])):
                            self.metadata['abstract'] = jsfile["abstract"]
                            self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"][j]["text"]))
                    else:
                        self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"]))
                        self.metadata['abstract'] = jsfile["abstract"]
                elif "abstract" in jsfile["metadata"].keys():
                    self.metadata['abstract'] = jsfile["metadata"]["abstract"]
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["metadata"]["abstract"]))
                else:
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=df.iloc[i].abstract))
                    self.metadata['abstract'] = self.df.iloc[i].abstract

                self.metadata['body_text'] = jsfile["body_text"]
                sections = list(set([k["section"] for k in jsfile["body_text"]]))

                for section in sections:
                    for l in range(len(jsfile["body_text"])):
                        if jsfile["body_text"][l]["section"] == section:
                            if section == '':
                                section = "body_text"
                            self.rows.append(dict(cord_uid=_id, section=section,
                                     subsection=l, text=jsfile["body_text"][l]["text"]))

                c = CORDCommons(self.directory)
                tables = c.extract_tables_from_json(jsfile)
                self.metadata['tables'] = tables
                for table in tables:
                    self.rows.append(dict(cord_uid=_id, section=table[0], subsection=table[1], text=table[2]))
                self.papers[doi] = self.metadata
            return
        
    def has_no_pdf_parse(self):
        self.papers = {}
        if self.directory:
            for i in self.df[(self.df["has_pmc_xml_parse"] == 0) & (self.df["has_pdf_parse"] == 0)].index:
                self.metadata = {}
                self.rows = []
                section = (str(self.df.iloc[i].full_text_file) + "/") * 2
                sha = self.df.iloc[i].sha
                doi = self.df.iloc[i].doi

                if len(sha.split("; ")) > 1:
                    sha = sha.split("; ")[0]
                filename = self.directory + '/' + section + "pdf_json/" + sha + ".json"
                self.metadata['path'] = section + "pdf_json/" + sha + ".json"
                print(filename)

                if len(sha) < 2:
                    bad_sha = True
                    try:
                        with open(directory + section + "pmc_json/" + df.iloc[i]["pmcid"] + ".xml.json") as paperjs:
                            jsfile = json.load(paperjs)
                    except:
                        pass
                if bad_sha == True:
                    bad_sha = False
                    #continue

                try:
                    with open(filename) as paperjs:
                        jsfile = json.load(paperjs)
                except:
                    print("Problem with ", self.df.iloc[i].cord_uid)
                    #continue

                _id = self.df.iloc[i]["cord_uid"]
                self.metadata['cord_uid'] = _id
                if "title" in jsfile.keys():
                    self.metadata['title'] = jsfile["title"]
                    rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                else:
                    rows.append(dict(cord_uid=_id, section="title", subsection=0, text=self.df.iloc[i].title))
                    self.metadata['title'] = self.df.iloc[i].title
                    
                if "abstract" in jsfile.keys():
                    if len(jsfile["abstract"]) > 1:
                        self.metadata['abstract'] = jsfile["abstract"]
                        for j in range(len(jsfile["abstract"])):
                            rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"][j]["text"]))
                    else:
                        self.metadata['abstract'] = jsfile["abstract"]
                        rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"]))
                elif "abstract" in jsfile["metadata"].keys():
                    self.metadata['abstract'] = jsfile["metadata"]["abstract"]
                    rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["metadata"]["abstract"]))
                else:
                    self.metadata['abstract'] = self.df.iloc[i].abstract
                    rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=self.df.iloc[i].abstract))

                sections = list(set([k["section"] for k in jsfile["body_text"]]))

                for section in sections:
                    self.metadata['body_text'] = jsfile["body_text"]
                    for l in range(len(jsfile["body_text"])):
                        if jsfile["body_text"][l]["section"] == section:
                            if section == '':
                                section = "body_text"
                            rows.append(dict(cord_uid=_id, section=section,
                                     subsection=l, text=jsfile["body_text"][l]["text"]))
                
                c = CORDCommons(self.directory)
                tables = c.extract_tables_from_json(jsfile)
                self.metadata['tables'] = tables
                for table in tables:
                    rows.append(dict(cord_uid=_id, section=table[0], subsection=table[1], text=table[2]))
                self.papers[doi] = self.metadata
        return

    def has_v22_parse(self):
        self.papers = {}
        if self.directory:
            for i in self.df.index:
                self.metadata = {}
                self.rows = []
                jsfile = ''
                jsonfiles = re.split("( )", str(self.df.iloc[i].pdf_json_files))
                jsonpath = jsonfiles
                section = (jsonpath[0] + "/") * 2
                sha = self.df.iloc[i].sha
                doi = self.df.iloc[i].doi
                # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,mag_id,who_covidence_id,arxiv_id,pdf_json_files,pmc_json_files,url,s2_id
                self.metadata['who_covidence_id'] = self.df.iloc[i].who_covidence_id
                self.metadata['source_x'] = self.df.iloc[i].source_x
                self.metadata['pmcid'] = self.df.iloc[i].pmcid
                self.metadata['pubmed_id'] = self.df.iloc[i].pubmed_id
                self.metadata['license'] = self.df.iloc[i].license
                self.metadata['publish_time'] = self.df.iloc[i].publish_time
                self.metadata['authors'] = self.df.iloc[i].authors
                self.metadata['journal'] = self.df.iloc[i].journal
                self.metadata['mag_id'] = self.df.iloc[i].mag_id
                self.metadata['arxiv_id'] = self.df.iloc[i].arxiv_id
                self.metadata['s2_id'] = self.df.iloc[i].s2_id
                self.metadata['year'] = re.split('-', self.metadata['publish_time'])[0]

                if len(sha.split("; ")) > 1:
                    sha = sha.split("; ")[0]
                filename = self.directory + '/' + section + "pdf_json/" + sha + ".json"
                self.metadata['path'] = section + "pdf_json/" + sha + ".json"
                filename = self.directory + '/' + jsonpath[0]

                bad_sha = False
                if len(sha) < 2:
                    bad_sha = True
                    try:
                        with open(directory + section + "pmc_json/" + df.iloc[i]["pmcid"] + ".xml.json") as paperjs:
                            jsfile = json.load(paperjs)
                    except:
                        pass
                if bad_sha == True:
                    bad_sha = False
                    #continue

                print("Open file %s" % filename)
                try:
                    with open(filename) as paperjs:
                        jsfile = json.load(paperjs)
                except:
                    print("Problem with ", self.df.iloc[i].cord_uid)
                    continue

                _id = self.df.iloc[i]["cord_uid"]
                self.metadata['cord_uid'] = _id
                if "title" in jsfile.keys():
                    self.metadata['title'] = jsfile["title"]
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                else:
                    self.rows.append(dict(cord_uid=_id, section="title", subsection=0, text=self.df.iloc[i].title))
                    self.metadata['title'] = self.df.iloc[i].title
    
                if "abstract" in jsfile.keys():
                    if len(jsfile["abstract"]) > 1:
                        self.metadata['abstract'] = jsfile["abstract"]
                        for j in range(len(jsfile["abstract"])):
                            self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"][j]["text"]))
                    else:
                        self.metadata['abstract'] = jsfile["abstract"]
                        self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"]))
                elif "abstract" in jsfile["metadata"].keys():
                    self.metadata['abstract'] = jsfile["metadata"]["abstract"]
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["metadata"]["abstract"]))
                else:
                    self.metadata['abstract'] = self.df.iloc[i].abstract
                    self.rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=self.df.iloc[i].abstract))

                sections = list(set([k["section"] for k in jsfile["body_text"]]))

                for section in sections:
                    self.metadata['body_text'] = jsfile["body_text"]
                    for l in range(len(jsfile["body_text"])):
                        if jsfile["body_text"][l]["section"] == section:
                            if section == '':
                                section = "body_text"
                            self.rows.append(dict(cord_uid=_id, section=section,
                                     subsection=l, text=jsfile["body_text"][l]["text"]))

                c = CORDCommons(self.directory)
                tables = c.extract_tables_from_json(jsfile)
                self.metadata['tables'] = tables
                for table in tables:
                    self.rows.append(dict(cord_uid=_id, section=table[0], subsection=table[1], text=table[2]))
                #self.papers[doi] = self.metadata

                self.metadata['body_rows'] = self.rows
                if doindex == 'mongo':
                    try:
                        self.collection.insert_one(self.metadata)
                    except:
                        print("errors with doi %s" % doi)
                else:
                    self.papers[doi] = self.metadata
        return
