import os
import pandas as pd
import json

class CORDCommons(): 
    def __init__(self, data_dir):
        self.map2file = self.create_map2file(data_dir)
        self.directory = data_dir
    
    def create_map2file(self,data_dir):
        map2file = dict()
        for dirname, _, filenames in os.walk(data_dir):
            for filename in filenames:
                name = filename.split('.')
                if len(name) > 1 and name[1] == 'json':
                    map2file[name[0]] = os.path.join(dirname, filename)
        return map2file
    
    def prep_data(self,file_list=None):
        if file_list==None:
            files = list(self.map2file)
        else:
            files = file_list
        for file_id in files:
            '''
            Generator providing section with labels
                0  _id  Section_name Text
                1
                2
            '''
            past_sec = None
            with open(self.map2file[file_id]) as paperjs:
                jsfile = json.load(paperjs)
                yield extract_title_from_json(jsfile)
                yield extract_abstract_from_json(jsfile) 
                for _,section in enumerate(jsfile['body_text']):
                    if past_sec != None and past_sec != section['section']:
                        #print('{} and{}'.format(past_sec,section))
                        past_sec = section['section']
                    yield [file_id,section['section'],section['text']]
                tables = extract_tables_from_json(jsfile)
                for i in tables: 
                    yield i
    #    Configuration.__init__(self)
    # Returns a dictionary object that's easy to parse in pandas.
    # For text mining purposes, we're only interested in 4 columns:
    # abstract, paper_id (for ease of indexing), title, and body text.
    # In this particular dataset, some abstracts have multiple sections,
    # with ["abstract"][1] or later representing keywords or extra info.
    # We only want to keep [0]["text"] in these cases.
        self.json_dict = {}
        self.json_dict_list = []

        self.filter_dict = {
    "discussion": ["conclusions","conclusion",'| discussion', "discussion",  'concluding remarks',
                   'discussion and conclusions','conclusion:', 'discussion and conclusion',
                   'conclusions:', 'outcomes', 'conclusions and perspectives',
                   'conclusions and future perspectives', 'conclusions and future directions'],
    "results": ['executive summary', 'result', 'summary','results','results and discussion','results:',
                'comment',"findings"],
    "introduction": ['introduction', 'background', 'i. introduction','supporting information','| introduction'],
    "methods": ['methods','method','statistical methods','materials','materials and methods',
                'data collection','the study','study design','experimental design','objective',
                'objectives','procedures','data collection and analysis', 'methodology',
                'material and methods','the model','experimental procedures','main text',],
    "statistics": ['data analysis','statistical analysis', 'analysis','statistical analyses',
                   'statistics','data','measures'],
    "clinical": ['diagnosis', 'diagnostic features', "differential diagnoses", 'classical signs','prognosis', 'clinical signs', 'pathogenesis',
                 'etiology','differential diagnosis','clinical features', 'case report', 'clinical findings',
                 'clinical presentation'],
    'treatment': ['treatment', 'interventions'],
    "prevention": ['epidemiology','risk factors'],
    "subjects": ['demographics','samples','subjects', 'study population','control','patients',
               'participants','patient characteristics'],
    "animals": ['animals','animal models'],
    "abstract": ["abstract", 'a b s t r a c t','author summary'],
    "review": ['review','literature review','keywords']}

    def extract_title_from_json(self, js):
        self.json_dict = [
            js["paper_id"],
            "title",
            js["metadata"]["title"],
            ]
        return self.json_dict
    
    # Returns a dictionary object that's easy to parse in pandas. For tables! :D
    def extract_tables_from_json(self, js):
        self.json_dict_list = []
        # Figures contain useful information. Since NLP doesn't handle images and tables,
        # we can leverage this text data in lieu of visual data.
        for figure in list(js["ref_entries"].keys()):
            self.json_dict = [
                js["paper_id"],
                figure,
                js["ref_entries"][figure]["text"]]
            self.json_dict_list.append(self.json_dict)
        return self.json_dict_list

    def extract_abstract_from_json(self,js):
        # In this particular dataset, some abstracts have multiple sections,
        # with ["abstract"][1] or later representing keywords or extra info.
        # We only want to keep [0]["text"] in these cases.
        if len(js["abstract"]) > 0:
            self.json_dict = [
                js["paper_id"],
                "abstract",
                js["abstract"][0]["text"]
            ]
            return self.json_dict

        # Else, ["abstract"] isn't a list and we can just grab the full text.
        else:
            self.json_dict = [
                js["paper_id"],
                "abstract",
                js["abstract"],
            ]

            return self.json_dict

    def process_delta(self):
        rows = []
        print(directory)
        if directory[-1] != "/":
            directory = directory + "/"

        df1 = pd.read_csv(directory + "metadata_old.csv")
        df2 = pd.read_csv(directory + "metadata.csv")
        df = df2[~df2["cord_uid"].isin(df1["cord_uid"])]
        df.reset_index(drop=True, inplace=True)
        del df1
        del df2
        df.fillna("~", inplace=True)
        return df
    
    def load_metadata(self):
        rows = []
        if self.directory[-1] != "/":
            self.directory = self.directory + "/"

        df = pd.read_csv(self.directory + "metadata.csv")
        df.reset_index(drop=True, inplace=True)
        df.fillna("~", inplace=True)
        return df    

    def invert_dict(self,d):
        inverse = dict()
        for key in d:
        # Go through the list that is saved in the dict:
            for item in d[key]:
            # Check if in the inverted dict the key exists
                if item not in inverse:
                # If not create a new list
                    inverse[item] = [key]
                else:
                    inverse[item].append(key)
        return inverse
        
    # Usage: inverted_dict = invert_dict(filter_dict)        
    def get_section_name(self,text):
        if len(text) == 0:
            return(text)
        text = text.lower()
        if text in inverted_dict.keys():
            return(inverted_dict[text][0])
        else:
            if "case" in text or "study" in text:
                return("methods")
            elif "clinic" in text:
                return("clinical")
            elif "stat" in text:
                return("statistics")
            elif "intro" in text or "backg" in text:
                return("introduction")
            elif "data" in text:
                return("statistics")
            elif "discuss" in text:
                return("discussion")
            elif "patient" in text:
                return("subjects")
            else:
                return(text)
    
    def init_ner(self):
        models = ["en_ner_craft_md", "en_ner_jnlpba_md","en_ner_bc5cdr_md","en_ner_bionlp13cg_md"]
        nlps = [spacy.load(model) for model in models]
        return(nlps)

    def gather_everything(self,data_dir):
        ex = Extraction(data_dir=data_dir)
        df_iter = ex.prep_data(None) 
        df_list =[j for j in [i for i in df_iter]]
        df = pd.DataFrame(columns=["paper_id","section","text"], data=df_list)
        df["section"] = [get_section_name(i) for i in df["section"]]
        return(df)
