from random import sample
import json

from pylathedb.keyword_match import KeywordMatch
from pylathedb.utils import get_logger

'''
Query match is a set of tuple-sets that, if properly joined,
can produce networks of tuples that fulfill the query. They
can be thought as the leaves of a Candidate Network.
'''

logger = get_logger(__name__)
class QueryMatch:

    def __init__(self, keyword_matches):
        self.keyword_matches = keyword_matches
        # self.value_score = 1.0
        # self.schema_score = 1.0
        # self.total_score = 1.0
        # self.tables_on_match = set()

    def get_km_from_keyword(self,keyword):
        #This is still somewhat random
        for keyword_match in self.keyword_matches:
            if keyword in keyword_match.keywords():
                return keyword_match
        return None

    def get_random_keyword_match(self):
        return sample(self.keyword_matches,k=1)[0]

    def calculate_total_score(self, value_index, schema_index, similarity, weight_scheme):
        self.total_score = 1

        has_value_terms = self.calculate_value_score(value_index, schema_index,weight_scheme)
        has_schema_terms = self.calculate_schema_score(similarity)

        if has_value_terms:
            self.total_score *= self.value_score

        if has_schema_terms:
            self.total_score *= self.schema_score

        self.total_score /= len(self)

        # logger.info("scores for : {} value_score: {} schema_score: {} tables on match: {} total: {}".format(self.keyword_matches,
        # self.value_score,
        # self.schema_score,
        # len(self.tables_on_match),
        # self.total_score))

    def calculate_schema_score(self,similarity):
        self.schema_score = 1
        has_schema_terms = False
        for keyword_match in self.keyword_matches:
            for table, attribute, schema_words in keyword_match.schema_mappings():
                has_schema_terms = True
                sim_values = [
                    similarity.word_similarity(term,table,attribute) 
                    for term in schema_words
                ]
                self.schema_score *= sum(sim_values)/len(sim_values)
        return has_schema_terms

    def calculate_value_score(self, value_index, schema_index, weight_scheme):
        has_value_terms = False
        self.value_score = 1

        for keyword_match in self.keyword_matches:
            for table, attribute, keywords in keyword_match.value_mappings():
                has_value_terms = True
                metrics = schema_index[table][attribute]
                tf_iaf_sum = 0
                # self.tables_on_match.add(table)
                for term in keywords:
                    tf = value_index.get_tf(weight_scheme,term,table,attribute,metrics['max_frequency'])
                    iaf = value_index.get_iaf(weight_scheme,term)
                    tf_iaf_sum+= (tf*iaf)

                cos = tf_iaf_sum / metrics['norm'][weight_scheme]
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
