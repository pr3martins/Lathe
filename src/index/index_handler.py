from .database_iter import DatabaseIter
from .value_index import ValueIndex
from .schema_index import SchemaIndex
from utils import ConfigHandler,get_logger
from ctypes import *
import json

logger = get_logger(__name__)

class IndexHandler:
    def __init__(self):
        self.config = ConfigHandler()
        self.value_index = ValueIndex()
        self.schema_index = SchemaIndex()
        self.vocab = []
        self.vocal = {}

    def create_histogram(self):
        max_count = -1
        if not self.config.create_index:
            return

        database_iter = DatabaseIter()
        print(self.config.remove_from_index)
       
        for table,ctid,attribute,word in database_iter:
            if table not in self.config.remove_from_index:
                if word not in vocab_set:
                    vocab_set.add(word)

                # vocab = []
                # vocab_item = self.word_histogram.setdefault(word, {})
                # count = vocab_item.setdefault('{}.{}'.format(table, attribute), 1)
                # vocab_item['{}.{}'.format(table, attribute)] = count + 1
                
                if count > max_count:
                    max_count = count
                    self.word_choices[0] = word
                    self.word_choices[1] = '{}.{}'.format(table, attribute)
        
        
        print(self.word_choices)        
                
    
    def create_index_file(self):
        tables = {}
        #attributes = set()
        #first interaction, discover the number of tables, attributes, and vocabulary
        database_iter = DatabaseIter()
        for table, ctid, attribute,word in database_iter:
            if table not in self.config.remove_from_index:
                idx = self.schema_index.add(table, attribute)
                self.value_index.count_attributes(idx, word)
        
        self.value_index.create_file(len(self.schema_index.attributes))
        
    def create_indexes(self):
        if not self.config.create_index:
            return

        database_iter = DatabaseIter()
        for table,ctid,attribute, word in database_iter:
            if table not in self.config.remove_from_index:
                self.value_index.add_mapping(word,table,attribute,ctid)
                self.schema_index.increment_frequency(word,table,attribute)
                
        
        self.schema_index.process_frequency_metrics()
        num_total_attributes = self.schema_index.get_number_of_attributes()
        self.value_index.process_iaf(num_total_attributes)
        self.schema_index.process_norms_of_attributes(self.value_index.frequencies_iafs())

    def dump_indexes(self,value_index_filename,schema_index_filename):
        self.value_index.persist_to_shelve(value_index_filename)
        self.schema_index.persist_to_shelve(schema_index_filename)

    def load_indexes(self,value_index_filename,schema_index_filename,**kwargs):
        self.value_index = self.value_index.load_from_shelve(value_index_filename,**kwargs)
        self.schema_index = self.schema_index.load_from_shelve(schema_index_filename)
