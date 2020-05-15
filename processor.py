import pandas as pd 
import os
import numpy as np
from commons import CORDCommons
import json

class CORDProcessor():
    def __init__(self, df, directory):    
        self.df = df
        self.directory = directory
        
    def has_pmc_xml_parse(self):
        if self.directory:
            jsfile = {}
            self.papers = {}
            self.rows = []
            for i in self.df[self.df["has_pmc_xml_parse"] == 1].index:
                self.metadata = {}
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
            self.rows = []
            self.papers = {}
            jsfile = {}
            for i in self.df[(self.df["has_pmc_xml_parse"] == 0) & (df["has_pdf_parse"] == 1)].index:
                self.metadata = {}
                section = (str(df.iloc[i].full_text_file) + "/") * 2
                doi = df.iloc[i].doi
                sha = df.iloc[i].sha
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
                    rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                else:
                    rows.append(dict(cord_uid=_id, section="title", subsection=0, text=df.iloc[i].title))
                    self.metadata['title'] = df.iloc[i].title
                    
                if "abstract" in jsfile.keys():
                    if len(jsfile["abstract"]) > 1:
                        for j in range(len(jsfile["abstract"])):
                            self.metadata['abstract'] = jsfile["abstract"]
                            rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"][j]["text"]))
                    else:
                        rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["abstract"]))
                        self.metadata['abstract'] = jsfile["abstract"]
                elif "abstract" in jsfile["metadata"].keys():
                    self.metadata['abstract'] = jsfile["metadata"]["abstract"]
                    rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=jsfile["metadata"]["abstract"]))
                else:
                    rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=df.iloc[i].abstract))
                    self.metadata['abstract'] = df.iloc[i].abstract

                self.metadata['body_text'] = jsfile["body_text"]
                sections = list(set([k["section"] for k in jsfile["body_text"]]))

                for section in sections:
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
                    self.rows.append(dict(cord_uid=_id, section=table[0], subsection=table[1], text=table[2]))
                self.papers[doi] = self.metadata
            return
        
    def has_no_pdf_parse(self):
        self.papers = {}
        if self.directory:
            for i in df[(df["has_pmc_xml_parse"] == 0) & (df["has_pdf_parse"] == 0)].index:
                self.metadata = {}
                section = (str(df.iloc[i].full_text_file) + "/") * 2
                sha = df.iloc[i].sha
                doi = df.iloc[i].doi

                if len(sha.split("; ")) > 1:
                    sha = sha.split("; ")[0]
                filename = self.directory + '/' + section + "pdf_json/" + sha + ".json"
                self.metadata['path'] = section + "pdf_json/" + sha + ".json"

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
                    print("Problem with ", df.iloc[i].cord_uid)
                    #continue

                _id = df.iloc[i]["cord_uid"]
                self.metadata['cord_uid'] = _id
                if "title" in jsfile.keys():
                    self.metadata['title'] = jsfile["title"]
                    rows.append(dict(cord_uid=_id, section="title", subsection=0, text=jsfile["title"]))
                else:
                    rows.append(dict(cord_uid=_id, section="title", subsection=0, text=df.iloc[i].title))
                    self.metadata['title'] = df.iloc[i].title
                    
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
                    self.metadata['abstract'] = df.iloc[i].abstract
                    rows.append(dict(cord_uid=_id, section="abstract", subsection=0, text=df.iloc[i].abstract))

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
