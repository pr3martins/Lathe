#!/usr/bin/env python
# -*- coding: utf-8 -*-
import shelve
from math import log1p, sqrt

from .babel_hash import BabelHash
from utils import get_logger

logger = get_logger(__file__)
class ValueIndex(dict):

    def __init__(self,*args):
        dict.__init__(self,args)

    def add_mapping(self,word,table,attribute,ctid):
        self.setdefault( word, (0, BabelHash() ) )
        self[word].setdefault(table , BabelHash() )
        self[word][table].setdefault( attribute , [] ).append(ctid)

    def get_mappings(self,word,table,attribute):
        return self[word][table][attribute]

    def get_iaf(self,key):
        return dict.__getitem__(self,key)[0]

    def set_iaf(self,key,IAF):
        oldIAF,oldValue = dict.__getitem__(self,key)
        dict.__setitem__(self, key,  (IAF,oldValue))

    def get_frequency(self,word,table,attribute):
        return len(self[word][table][attribute])

    def frequencies_iafs(self):
        for word in self:
            iaf = self.get_iaf(word)
            for table in self[word]:
                for attribute, ctids in self[word][table].items():
                    frequency = len(ctids)
                    yield table,attribute,frequency,iaf

    def __getitem__(self,word):
        return dict.__getitem__(self,word)[1]

    def __setitem__(self,word,value):
        #TODO Nao permitir setar o item sem si, apenas atraves de add_mapping
        raise Error

    def _set_underlying_item(self, key, value):
        return dict.__setitem__(self, key, value)

    def process_iaf(self,num_total_attributes):
        for word, values in self.items():
            num_attributes_with_this_word = sum([len(attribute) for table,attribute in self[word].items()])
            IAF = log1p(num_total_attributes/num_attributes_with_this_word)
            self.set_iaf(word,IAF)
        logger.info('IAF PROCESSED')

    def persist_to_shelve(self,filename):
        with shelve.open(filename) as storage:
            for key,value in self.items():
                storage[key]=value
    @staticmethod
    def load_from_shelve(filename,**kwargs):
        value_index = ValueIndex()
        with shelve.open(filename,flag='r') as storage:
            for keyword in kwargs.get('keywords',storage.keys()):
                try:
                    value_index._set_underlying_item(keyword,storage[keyword])
                except KeyError:
                    continue
        return value_index
