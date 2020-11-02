import  itertools

from utils import ConfigHandler
from utils import get_logger
from keyword_match import KeywordMatch

from .query_match import QueryMatch

logger = get_logger(__name__)

class QueryMatchHandler:

    def __init__(self):
        self.query_matches = []

    def generate_query_matches(self, keyword_query,keyword_matches, **kwargs):
        #Input:  A keyword query Q, The set of non-empty non-free tuple-sets Rq
        #Output: The set Mq of query matches for Q
        max_qm_size = kwargs.get('max_qm_size',3)
        segmes = kwargs.get('segments', [])
        self.query_matches = []
        for i in range(1,max(len(keyword_query), max_qm_size)+1):
            for candidate_match in itertools.combinations(keyword_matches,i):
                #print("candidate query match: {}".format(candidate_match))
                if self.has_minimal_cover(candidate_match,keyword_query):
                #and \
                #(len(sements) != 0  and self.check_segments(segments, candidate_match):
                    merged_query_match = self.merge_schema_filters(candidate_match)

                    query_match = QueryMatch(merged_query_match)

                    #TODO: checking user group
                    self.query_matches.append(query_match)

    def has_minimal_cover(self, candidate_match, keyword_query):
        #Input:  A subset CM (Candidate Query Match) to be checked as total and minimal cover
        #Output: If the match candidate is a TOTAL and MINIMAL cover

        subset = [kw_match.keywords() for kw_match in candidate_match]
        u = set().union(*subset)

        is_total = (u == set(keyword_query))
        for element in subset:

            new_u = list(subset)
            new_u.remove(element)

            new_u = set().union(*new_u)

            if new_u == set(keyword_query):
                return False

        return is_total


    def merge_schema_filters(self, query_matches):
        table_hash={}
        for keyword_match in query_matches:
            joint_schema_filter,value_keyword_matches = table_hash.setdefault(keyword_match.table,({},set()))

            for attribute, keywords in keyword_match.schema_filter:
                joint_schema_filter.setdefault(attribute,set()).update(keywords)

            if len(keyword_match.value_filter) > 0:
                value_keyword_matches.add(keyword_match)

        merged_qm = set()
        for table,(joint_schema_filter,value_keyword_matches) in table_hash.items():
            if len(value_keyword_matches) > 0:
                joint_value_filter = {attribute:keywords
                                    for attribute,keywords in value_keyword_matches.pop().value_filter}
            else:
                joint_value_filter={}

            joint_keyword_match = KeywordMatch(table,
                                            value_filter=joint_value_filter,
                                            schema_filter=joint_schema_filter)

            merged_qm.add(joint_keyword_match)
            merged_qm.update(value_keyword_matches)

        return merged_qm

    def rank_query_matches(self, value_index, schema_index, similarity, log_score=False):
        for query_match in self.query_matches:
            logger.debug("query match: {}".format(query_match))
            query_match.calculate_total_score(value_index,schema_index, similarity, log_score)

        self.query_matches.sort(key=lambda query_match: query_match.total_score,reverse=True)
