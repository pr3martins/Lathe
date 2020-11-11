import shelve
import gc
import math

from utils import get_logger

from .babel_hash import BabelHash

logger = get_logger(__file__)
class SchemaIndex():
    def __init__(self):
        self._dict = {}

    def __iter__(self):
        yield from self._dict.keys()

    def __getitem__(self,word):
        return self._dict[word]

    def __setitem__(self,key,value):
        self._dict[key] = value

    def keys(self):
        yield from self.__iter__()

    def items(self):
        for key in self.__iter__():
            yield key, self.__getitem__(key)

    def values(self):
        for key in self:
            yield self[key]

    def create_entries(self,table_attributes):
        for (table,attribute) in table_attributes:
            self._dict.setdefault(table,{}).setdefault(attribute,{'max_frequency':0, 'norm':0.0})

    def tables_attributes(self):
        return {(table,attribute) for table in self for attribute in self[table]}

    def get_num_total_attributes(self):
        return sum([len(attribute) for attribute in self.values()])

    def persist_to_shelve(self,filename):
        with shelve.open(filename) as storage:
            for key,value in self._dict.items():
                storage[key]=value

    @staticmethod
    def load_from_shelve(filename):
        schema_index = SchemaIndex()
        with shelve.open(filename,flag='r') as storage:
            for key,value in storage.items():
                schema_index[key]=value
        return schema_index
