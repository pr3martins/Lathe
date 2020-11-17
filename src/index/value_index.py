import shelve
from math import log1p, sqrt
from random import sample

from utils import get_logger

from .babel_hash import BabelHash

logger = get_logger(__file__)
class ValueIndex():

    def __init__(self,**kwargs):
        self._dict = {}
        self.persistant_filename = kwargs.get('persistant_filename',None)

    def __contains__(self, keyword):
        if keyword in self._dict:
            return True

        if self.persistant_filename:
            with shelve.open(self.persistant_filename,flag='r') as storage:
                if keyword in storage:
                    return True

        return False

    def _set_underlying_item(self, keyword, value):
        self._dict[keyword]=value

    def _get_underlying_item(self, keyword, value):
        item = None
        if keyword in self._dict:
            item = self._dict[keyword]
        elif self.persistant_filename:
            with shelve.open(self.persistant_filename,flag='r') as storage:
                item = storage[keyword]
        if item is None:
            raise KeyError('The keyword {keyword} is not in the ValueIndex.')
        return item

    def __getitem__(self,keyword):
        iaf, value = self._get_underlying_item(keyword)
        return value

    def get_iaf(self,key):
        iaf, value = self._get_underlying_item(keyword)
        return iaf

    def __setitem__(self,keyword,value):
        raise Exception('Invalid operation. ValueIndex items shoud be set using the add_mapping method.')

    def __iter__(self):
        #This method only iterates over the keywords in memory
        yield from self._dict.keys()

    def keys(self):
        yield from self.__iter__()

    def items(self):
        for key in self.__iter__():
            yield key, self.__getitem__(key)

    def get_mappings(self,keyword,table,attribute):
        return self[keyword][table][attribute]

    def add_mapping(self,keyword,table,attribute,ctid):
        # This method does not change the persistant_filename
        self._dict.setdefault( keyword, (0, BabelHash() ) )
        self[keyword].setdefault(table , BabelHash() )
        self[keyword][table].setdefault( attribute , [] ).append(ctid)

    def set_iaf(self,keyword,iaf):
        # This method does not change the persistant_filename
        old_iaf,old_value = self._dict[keyword]
        self._dict[keyword]= (iaf,old_value)

    def get_frequency(self,keyword,table,attribute):
        return len(self[keyword][table][attribute])

    def frequencies_iafs(self):
        for word in self:
            iaf = self.get_iaf(word)
            for table in self[word]:
                for attribute, ctids in self[word][table].items():
                    frequency = len(ctids)
                    yield table,attribute,frequency,iaf

    def __repr__(self):
        return f'<ValueIndex {repr(self._dict)}>'

    def persist_to_shelve(self,filename):
        with shelve.open(filename) as storage:
            storage['__babel__']=BabelHash.babel
            for key,underlying_value in self._dict.items():
                #print(f'Persist {key}={underlying_value}')
                storage[key]=underlying_value

    def load_from_shelve(self,persistant_filename,**kwargs):
        self.persistant_filename = persistant_filename
        #If the sample_size == 0, this method loads all keywords from persistant_filename
        sample_size = kwargs.get('sample_size',0)
        with shelve.open(self.persistant_filename,flag='r') as storage:
            keywords = kwargs.get('keywords',storage.keys())

            if '__babel__' in storage:
                BabelHash.babel.update(storage['__babel__'])

            if sample_size > 0:
                keywords = sample(keywords, k = sample_size)

            for keyword in keywords:
                if keyword == '__babel__':
                    continue
                try:
                    self._set_underlying_item(keyword,storage[keyword])
                except KeyError:
                    print(f'Keyword {keyword} not present in persistant_filename')
                    continue
