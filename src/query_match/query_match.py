
import re


'''
Query match is a set of tuple-sets that, if properly joined,
can produce networks of tuples that fulfill the query. They
can be thought as the leaves of a Candidate Network.      
'''
class QueryMatch:
    
    def __init__(self, matches=[]):
        self.matches = matches
        self.value_score = 1.0
        self.schema_score = 1.0
        self.total_score = 1.0
        self.tables_on_match = set()
        
    def calculate_total_score(self, value_index, schema_index, similarity, log_mode=False):
        
        has_value_terms = self.calculate_value_score(value_index, schema_index)    
        has_schema_terms self.calculate_schema_score(schema_index, similarity)
        
        if has_value_terms:
            if log_mode:
                self.total_score += self.value_score
            else:
                self.total_score *= self.value_score

        if has_score_terms:
            if log_mode:
                self.total_score += self.value_score
            else:
                self.total_score *= self.value_score
        
        if log_score:
                score += log(1) - log(len(table_set))
            else:
                score *= 1./ (1. * len(table_set)) 
        
    def calculate_schema_score(self,schema_index, similarity,split_text=False):
        has_schema_terms = False
        for table, attribute, schemaWords in ts.schema_mappings():
            self.tables_on_match.add(table)
            schemasum = 0

            pattern = re.compile('[_,-]')
            attributes = if not split_text [attribute] else pattern.split(attribute)
            tables = if not split_text [table] else else pattern.split(tables)

            max_sim = 0
            for term in schemaWords:
                for attribute_item in attributes:
                    for table_item in tables:

                        sim = sm.word_similarity(term,table_item,attribute_item, get_average=True)
                        logger.debug('similarity btw {0} ({1}) {2} {3}'.format(term, table_item, attribute_item, sim))
                        if sim > max_sim:
                            max_sim = sim
                
                schemasum += max_sim    
                has_schema_terms = True
            
            if log_score:
                self.schema_score += log(schemasum)
            else:
                self.schema_score *= schemasum
                
        return has_schema_terms
    
    def calculate_value_score(self, value_index, schema_index, log_score):
        has_value_terms = False
        for keyword_mach in self.matches:
            for table, attribute, keywords in keyword_match.value_mappings():
                
                norm = schema_index[table][attribute]['norm']
                num_words = schema_index[table][attribute]['num_words']
                num_distinct_words = schema_index[table][attribute]['num_distinct_words']
                max_frequency  = schema_index[table][attribute]['max_frequency']
                
                wsum = 0.0
                m_words = set()
                self.tables_on_match.add(table)
                for term in keywords:        
                    iaf = value_index.get_iaf(term)
                    tf = 0.0
                    frequency = len(value_index.mappings(term,table,attribute))
                    word_set = set(value_index.mappings(term, table,attribute))
                    
                    if log_score:
                        tf = log(frequency) - log(maxFrequency)
                        wsum = wsum + tf + log(float(IAF))
                    else:    
                        tf = (float(frequency) * 1.) / (float(maxFrequency) *1.)
                        wsum = wsum + tf*float(iaf)
                    
                    has_value_terms = True

                    if len(mWords) == 0:
                        m_words = word_set
                    else:
                        m_words = m_words & word_set
                
                if log_score:
                    cos = wsum - log(float(Norm))
                    self.value_score += cos
                else:
                    cos = (float(wsum) / float(Norm))
                    self.value_score *= cos
                    
        return has_value_terms
    
    
    def __str__(self):
        return  'query_match : {}\ntotal_score: {}\nschema_score: {}\n:value_score: {}'.format(\
            ','.join(self.matches),self.total_score, self.schema_score, self.value_score)
        
