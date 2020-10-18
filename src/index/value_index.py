import shelve
from math import log1p, sqrt
from random import sample

from utils import get_logger

from .babel_hash import BabelHash

logger = get_logger(__file__)
class ValueIndex():

    def __init__(self,**kwargs):
        self._dict = {}

    def add_mapping(self,word,table,attribute,ctid):
        self.setdefault( word, (0, BabelHash() ) )
        self[word].setdefault(table , BabelHash() )
        self[word][table].setdefault( attribute , [] ).append(ctid)

    def get_mappings(self,word,table,attribute):
        return self[word][table][attribute]

    def get_iaf(self,key):
        item = None
        if self.value_index_file_desc is not None:
            with shelve.open(self.value_index_file_desc,flag='r') as storage:
                item = storage[key]
        else:
            item = self._dict[key]

        return item[0]

    def set_iaf(self,key,IAF):
        oldIAF,oldValue = self._dict[key]
        self._dict[key]= (IAF,oldValue)

    def get_frequency(self,word,table,attribute):
        return len(self[word][table][attribute])

    def __iter__(self):
        yield from self._dict.keys()

    def __contains__(self, keyword):
        contains = False
        with shelve.open(self.value_index_file_desc,flag='r') as storage:
            contains = keyword in storage
        return contains
    def keys(self):
        yield from self.__iter__()

    def items(self):
        for key in self.__iter__():
            yield key, self.__getitem__(key)

    def frequencies_iafs(self):
        for word in self:
            iaf = self.get_iaf(word)
            for table in self[word]:
                for attribute, ctids in self[word][table].items():
                    frequency = len(ctids)
                    yield table,attribute,frequency,iaf

    def __getitem__(self,word):
        item = None
        if self.value_index_file_desc is not None:
            with shelve.open(self.value_index_file_desc,flag='r') as storage:
                item = storage[word]
        else:
            item = self._dict[word]
        return item[1]

    def setdefault(self,key,default):
        return self._dict.setdefault(key,default)

    def __setitem__(self,word,value):
        #TODO Nao permitir setar o item sem si, apenas atraves de add_mapping
        raise Error

    def _set_underlying_item(self, key, value):
        self._dict[key]=value

    def __repr__(self):
        return repr(self._dict)

    def process_iaf(self,num_total_attributes):
        for word, values in self.items():
            num_attributes_with_this_word = sum([len(attribute) for table,attribute in self[word].items()])
            IAF = log1p(num_total_attributes/num_attributes_with_this_word)
            self.set_iaf(word,IAF)
        logger.info('IAF PROCESSED')

    def persist_to_shelve(self,filename):
        with shelve.open(filename) as storage:
            storage['__babel__']=BabelHash.babel

            for key,underlying_value in self._dict.items():
                #print(f'Persist {key}={underlying_value}')
                storage[key]=underlying_value
    
    def load_file(self, value_index_filename):
        self.value_index_file_desc = value_index_filename
        with shelve.open(self.value_index_file_desc,flag='r') as storage:
            if '__babel__' in storage:
                BabelHash.babel.update(storage['__babel__'])


    def get_mappings_from_file(self, keyword):
        with shelve.open(self.value_index_file_desc,flag='r') as storage:
            if keyword in storage:
                #self._set_underlying_item(keyword, storage[keyword])
                return storage[keyword]

        return (0, BabelHash())

    @staticmethod
    def load_from_shelve(filename,**kwargs):
        value_index = ValueIndex()
        with shelve.open(filename,flag='r') as storage:
            keywords = kwargs.get('keywords',storage.keys())
            sample_size = kwargs.get('sample_size',0)
            
            if '__babel__' in storage:
                BabelHash.babel.update(storage['__babel__'])

            if sample_size > 0:
                keywords = sample(keywords, k = sample_size)

            for keyword in keywords:
                try:
                    value_index._set_underlying_item(keyword,storage[keyword])
                except KeyError:
                    continue
        return value_index
