#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .babel_hash import *


class ValueIndex(dict):
        
    def __init__(self,*args): 
        dict.__init__(self,args)
    
    def load(self, word_hash, dict_word):

        for word in word_hash.keys():
            iaf = word_hash[word][0]
            
            for table in word_hash[word][1]:
                table_word = dict_word[table]
                for attribute in word_hash[word][1][table]:
                    attribute_word = dict_word[attribute]
                    for ids in word_hash[word][1][table][attribute]:
                        self.addMapping(word, table_word, attribute_word, ids)
            self.setIAF(word, iaf)               

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
    
    def __getitem__(self,word):
        return dict.__getitem__(self,word)[1]

    def get_all_notation(self, word):
        return dict.__getitem__(self,word)
    
    def __setitem__(self,word,value): 
        oldIAF,oldValue = dict.__getitem__(self,word)
        dict.__setitem__(self, word,  (oldIAF,value))

    def get_translate_mapping(self):
        keys = self.keys()
        return self[keys[0]].writeBabel()