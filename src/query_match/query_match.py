from random import sample
import re

from keyword_match import KeywordMatch
from utils import get_logger
from math import log

'''
Query match is a set of tuple-sets that, if properly joined,
can produce networks of tuples that fulfill the query. They
can be thought as the leaves of a Candidate Network.
'''

logger = get_logger(__name__)
class QueryMatch:

    def __init__(self, keyword_matches):
        self.keyword_matches = keyword_matches
        self.value_score = 1.0
        self.schema_score = 1.0
        self.total_score = 1.0
        self.tables_on_match = set()

    def get_random_keyword_match(self):
        return sample(self.keyword_matches,k=1)[0]


    def calculate_total_score(self, value_index, schema_index, similarity, log_score=False):

        has_value_terms = self.calculate_value_score(value_index, schema_index, log_score)
        has_schema_terms = self.calculate_schema_score(similarity, log_score)

        if has_value_terms:
            if log_score:
                self.total_score += self.value_score
            else:
                self.total_score *= self.value_score

        if has_schema_terms:
            if log_score:
                self.total_score += self.schema_score
            else:
                self.total_score *= self.schema_score

        if log_score:
            self.total_score += log(1) - log(len(self.tables_on_match))
        else:
           self.total_score *= 1./ (1. * len(self.tables_on_match))

        logger.info("scores for : {} value_score: {} schema_score: {} tables on match: {} total: {}".format(self.keyword_matches,
        self.value_score,
        self.schema_score,
        len(self.tables_on_match),
        self.total_score))

    def calculate_schema_score(self,similarity,log_score, split_text=True):
        has_schema_terms = False
        for keyword_match in self.keyword_matches:

            for table, attribute, schema_words in keyword_match.schema_mappings():
                self.tables_on_match.add(table)
                logger.debug('for {0}.{1}'.format(table, attribute))

                schemasum = 0

                pattern = re.compile('[_,-]')
                attributes =  [attribute] if not split_text else pattern.split(attribute)
                tables = [table] if not split_text else pattern.split(table)

                for term in schema_words:
                    max_sim = 0
                    for table_item in tables:
                        for attribute_item in attributes:
                            sim = similarity.word_similarity(term,table_item,attribute_item)
                            logger.info('similarity btw {0}: {1}.{2} {3}'.format(term, table_item, attribute_item, sim))
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
        for keyword_match in self.keyword_matches:
            for table, attribute, keywords in keyword_match.value_mappings():

                # TODO colocar metrics = schema_index[table][attribute] e mencionar como dict parece mais limpo
                metrics = schema_index[table][attribute]
                norm = schema_index[table][attribute]['norm']
                max_frequency  = schema_index[table][attribute]['max_frequency']

                wsum = 0.0
                m_words = set()
                self.tables_on_match.add(table)
                for term in keywords:
                    iaf = value_index.get_iaf(term)
                    tf = 0.0
                    frequency = value_index.get_frequency(term,table,attribute)
                    word_set = set(value_index.get_mappings(term, table,attribute))
                    #logger.debug("Frequency {}, term {}, max_frequency: {}, table {}, attr {}".format(frequency, term, max_frequency, table, attribute))
                    if log_score:
                        tf = log(frequency) - log(max_frequency)
                        wsum = wsum + tf + log(float(iaf))
                    else:
                        tf = (float(frequency) * 1.) / (float(max_frequency) *1.)
                        wsum = wsum + tf*float(iaf)

                    has_value_terms = True

                    if len(m_words) == 0:
                        m_words = word_set
                    else:
                        m_words = m_words & word_set

                if log_score:
                    cos = wsum - log(float(norm))
                    self.value_score += cos
                else:
                    cos = (float(wsum) / float(norm))
                    logger.debug("Debuggin word {} for attr: {}.{} previous score: {} current score: {}".format(term,table, attribute, self.value_score, cos))
                    self.value_score *= cos


        return has_value_terms

    def __eq__(self, other):
        return (isinstance(other, QueryMatch)
                and self.keyword_matches == other.keyword_matches)

    def __iter__(self):
        return iter(self.keyword_matches)

    def __len__(self):
        return len(self.keyword_matches)

    def __repr__(self):
        return repr(self.keyword_matches)

    def __str__(self):
        return repr(self.keyword_matches)

    def to_json_serializable(self):
        return [keyword_match.to_json_serializable() for keyword_match in self.keyword_matches]

    def to_json(self):
        return json.dumps(self.to_json_serializable())

    @staticmethod
    def from_json_serializable(json_serializable_qm):
        return QueryMatch({KeywordMatch.from_json_serializable(json_serializable_km)
                           for json_serializable_km in json_serializable_qm})

    @staticmethod
    def from_json(str_json):
        return QueryMatch.from_json_serializable(json.loads(str_json))
