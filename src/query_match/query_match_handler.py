import  itertools
from pprint import pprint as pp
import time
import re
from math import log
from utils import ConfigHandler
from utils import get_logger

logger = get_logger(__name__)

class QueryMatchHandler:
    
    def __init__(self):
        self.query_matches = []
        
        
    def has_minimal_cover(self, candidate_match, keyword_query):
        #Input:  A subset MC (Match Candidate) to be checked as total and minimal cover
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


    def generate_query_matches(self, keyword_query,keyword_matches):
        #Input:  A keyword query Q, The set of non-empty non-free tuple-sets Rq
        #Output: The set Mq of query matches for Q
        self.query_matches = []
        for i in range(1,len(keyword_query)+1):
            for candidate_match in itertools.combinations(keyword_matches,i): 
                logger.debug("candidate match: {}".format(candidate_match))   
                if self.has_minimal_cover(candidate_match,keyword_query):
                    M = self.find_query_match(set(candidate_match))
                    
                    query_match = QueryMatch(matches=candidate_match)
                    self.query_matches.append(candidate_match)
    
        
    def find_query_match(self, candidate_match):  
        something_changed = False 
        for kw_match_a, kw_match_b in itertools.combinations(candidate_match,2):
            
            kw_match_x = kw_match_a.union(kw_match_b, projectionOnly = True)

            if kw_match_x is not None:   
                M.add(kw_match_x)      
                M.remove(kw_match_a)
                M.remove(kw_match_b)
                
                return self.find_query_match(M)
            
        return M

    def rank_query_matches(self, value_index, schema_index, similarity, log_score=False):
        for query_match in self.query_matches:    
            query_match.calculate_total_score(value_index,schema_index, similarity, log_score)
                
        self.query_matches.sort(key=lambda query_match: query_match.total_score,reverse=True)

        for rank_item in query_matches[:10]:    
            logger.debug('--------------------')
            logger.debug(rank_item)
        logger.debug('--------------------')

            
        return Ranking