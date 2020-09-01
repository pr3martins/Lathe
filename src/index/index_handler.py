


from json import dump, load, JSONEncoder, JSONDecoder
from utils import ConfigHandler
from .database_iter import DatabaseIter
from .value_index import ValueIndex
from .schema_index import SchemaIndex
from utils import get_logger
from math import log1p, sqrt 
import pickle
import gc
import sys

logger = get_logger(__name__)
class IndexHandler:
    def __init__(self):
        self.config = ConfigHandler()
        
        
    def create_inverted_index(self):
        if not self.config.createIndex:
            return
        
        database_iter = DatabaseIter()
        self.value_index = ValueIndex()
        self.schema_index = SchemaIndex()
        
        for table,ctid,column, word in database_iter:
            self.value_index.add_mapping(word,table,column,ctid)
        
            self.schema_index.setdefault(table,{}).setdefault(column,{}).setdefault(word,1)
            self.schema_index[table][column][word]+=1
            
        for table in self.schema_index:
            for column in self.schema_index[table]:
            
            max_frequency = num_distinct_words = num_words = 0            
            for word, frequency in ah[table][column].items():
                
                num_distinct_words += 1
                
                num_words += frequency
                
                if frequency > max_frequency:
                    max_frequency = frequency
            
            norm = 0
            self.schema_index[table][column] = (norm,numDistinctWords,numWords,maxFrequency)
            
        logger.info('INVERTED INDEX CREATED')
        gc.collect()
        
    def get_iaf(self):
        total_attributes = sum([len(attribute) for attribute in self.schema_index.values()])
    
        for (term, values) in self.value_index.items():
            attributes_with_this_term = sum([len(attribute) for attribute in self.value_index[term].values()])
            IAF = log1p(total_attributes/attributes_with_this_term)
            self.value_index.setIAF(term,IAF)
        logger.info('IAF PROCESSED')
    
    def get_attributes_old_norm(self):
        for word in self.value_index:
            iaf = self.value_index.get_iaf(word)
            for table in self.value_index[word]:
                for column, ctids in self.value_index[word][table].items():
                    
                    (prev_norm,num_distinct_words,num_words,max_frequency) = self.schema_index[table][column]

                    frequency = len(ctids)
                    
                    TF = (frequency * 1.0 / maxFrequency * 1.0)
                    
                    norm = prev_norm + (TF*IAF)
                    self.schema_index[table][column]=(norm,num_distinct_words,num_words,max_frequency)
                
        logger.info('NORMS OF ATTRIBUTES PROCESSED')
    
    
    def get_attributes_norm(self):
        for word in self.value_index:
            iaf = self.value_index.get_iaf(word)
            for table in self.value_index[word]:
                for column, ctids in self.value_index[word][table].items():
                    
                    (prev_norm,num_distinct_words,num_words,max_frequency) = self.schema_index[table][column]
                    frequency = len(ctids)   
                    TF = (frequency * 1.0 / maxFrequency * 1.0)    
                    norm = prev_norm + (TF*IAF)**2
                    
                    self.schema_index[table][column]=(norm,num_distinct_words,num_words,max_frequency)
        
        for table in self.schema_index:
            for column in self.schema_index[table]:
                (prev_norm,num_distinct_words,num_words,max_frequency) = self.schema_index[table][column]
                norm = math.sqrt(prev_norm)
                self.schema_index[table][column]=(norm,num_distinct_words,num_words,max_frequency)
        logger.info('NORMS OF ATTRIBUTES PROCESSED')
        
    #PAULO TO IMPLEMENT    
    def dump_index(self):
        pass
        
    #PAULO TO IMPLEMENT
    def load_indexes(self):
        pass
    