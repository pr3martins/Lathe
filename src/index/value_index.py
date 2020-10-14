from utils import ConfigHandler,get_logger
from .value_structure import DynamicValueStructure
import json

class ValueIndex:
    def __init__(self):
        self.config = ConfigHandler()
        self.value_file = open(self.config.value_file, 'a+b')
        self.value_structure = None
        self.register_size = 0
        self.general_vocab = {}
        self.max_elements = ['', '']
        self.max_ctids = 0
        self.vocab_file = open(self.config.vocab_file, 'a+')

    def add_word(self, word):
        self.vocab.add(word)

    def create_file(self, attribute_size):
        #self.register_size = 4 + (4 + (2 * self.max_ctids)) * attribute_size
        
        self.config.attribute_count = attribute_size
        self.config.max_ctids_count = self.max_ctids
        self.config.register_size = 4 + (4 + (4 * self.max_ctids)) * attribute_size
        self.config.dump()
        words = set(self.general_vocab.keys())
        self.general_vocab = None
        self.vocab = {key: value for value, key in enumerate(words) }
        json.dump(self.vocab, self.vocab_file)
        

    def count_attributes(self, idx, word):
        vocab_item = self.general_vocab.setdefault(word, {})
        count = vocab_item.setdefault('{}'.format(idx), 0) + 1
        vocab_item['{}'.format(idx)] = count

        if count > self.max_ctids:
            self.max_ctids = count
            self.max_elements[0] = word
            self.max_elements[1] = '{}'.format(idx)
    
    def index_term(self, idx, word, ctid):
        term_id = self.vocab[word]
        self.value_file.seek(term_id*self.register_size)


        
        