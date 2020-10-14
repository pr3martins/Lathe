import shelve
import gc
import math
from .babel_hash import BabelHash
from utils import get_logger

logger = get_logger(__file__)
class SchemaIndex(dict):
    def __init__(self,*args):
        dict.__init__(self,args)

    def increment_frequency(self,word,table,attribute):
        self.setdefault(table,BabelHash()).setdefault(attribute,BabelHash()).setdefault('frequencies',BabelHash()).setdefault(word,0)
        self[table][attribute]['frequencies'][word]+=1

    def process_frequency_metrics(self):
        for table in self:
            for attribute in self[table]:
                metrics = self[table][attribute]

                metrics['max_frequency']=0
                metrics['num_distinct_words']=0
                metrics['num_words']=0

                for word, frequency in metrics['frequencies'].items():
                    metrics['num_distinct_words'] += 1
                    metrics['num_words'] += frequency

                    if frequency >  metrics['max_frequency']:
                         metrics['max_frequency'] = frequency


    def clear_frequencies(self):
        for table in self:
            for attribute in self[table]:
                del self[table][attribute]['frequencies']
        gc.collect()

    def get_number_of_attributes(self):
        return sum([len(attribute) for attribute in self.values()])

    def process_norms_of_attributes(self,frequencies_iafs):
        for table,attribute,frequency,iaf in frequencies_iafs:
            prev_norm = self[table][attribute].setdefault('norm',0)
            max_frequency = self[table][attribute]['max_frequency']
            term_frequency = (frequency * 1.0 / max_frequency * 1.0)
            self[table][attribute]['norm'] = prev_norm + (term_frequency*iaf)**2


        for table in self:
            for attribute in self[table]:
                prev_norm = self[table][attribute]['norm']
                self[table][attribute]['norm'] = math.sqrt(prev_norm)
        logger.info('NORMS OF ATTRIBUTES PROCESSED')

    def persist_to_shelve(self,filename):
        with shelve.open(filename) as storage:
            for key,value in self.items():
                storage[key]=value

    @staticmethod
    def load_from_shelve(filename):
        schema_index = SchemaIndex()
        with shelve.open(filename,flag='r') as storage:
            for key,value in storage.items():
                schema_index[key]=value
        return schema_index
