import spacy
from tqdm.notebook import tqdm
from scipy.spatial import distance
import ipywidgets as widgets
from scispacy.abbreviation import AbbreviationDetector
from spacy_langdetect import LanguageDetector
# UMLS linking will find concepts in the text, and link them to UMLS. 
from scispacy.umls_linking import UmlsEntityLinker
import time
from spacy.vocab import Vocab

class CoronaNLP(): 
    def __init__(self):
        self.nlp = spacy.load("en_core_sci_lg", disable=["tagger"])
        self.nlp.max_length=2000000

        # We also need to detect language, or else we'll be parsing non-english text 
        # as if it were English. 
        self.nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)

        # Add the abbreviation pipe to the spacy pipeline. Only need to run this once.
        abbreviation_pipe = AbbreviationDetector(self.nlp)
        self.nlp.add_pipe(abbreviation_pipe)

        # Our linker will look up named entities/concepts in the UMLS graph and normalize
        # the data for us. 
        self.linker = UmlsEntityLinker(resolve_abbreviations=True)
        self.nlp.add_pipe(self.linker)
    
        new_vector = self.nlp(
               """Positive-sense single‐stranded ribonucleic acid virus, subgenus 
                   sarbecovirus of the genus Betacoronavirus. 
                   Also known as severe acute respiratory syndrome coronavirus 2, 
                   also known by 2019 novel coronavirus. It is 
                   contagious in humans and is the cause of the ongoing pandemic of 
                   coronavirus disease. Coronavirus disease 2019 is a zoonotic infectious 
                   disease.""").vector

        vector_data = {"COVID-19": new_vector,
               "2019-nCoV": new_vector,
               "SARS-CoV-2": new_vector}

        vocab = Vocab()
        for word, vector in vector_data.items():
            self.nlp.vocab.set_vector(word, vector)
        return

    def init_ner(self):
        models = ["en_ner_craft_md", "en_ner_jnlpba_md","en_ner_bc5cdr_md","en_ner_bionlp13cg_md"]
        self.nlps = [spacy.load(model) for model in models]
        return
