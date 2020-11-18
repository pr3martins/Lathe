import itertools
import re

from utils import ConfigHandler,get_logger, stopwords
from utils import Similarity

from .keyword_match import KeywordMatch

logger = get_logger(__name__)
class KeywordMatchHandler:
    def __init__(self, similarity):
        self.config = ConfigHandler()
        self.similarities = similarity

    def value_keyword_match_generator(self, Q,value_index,**kwargs):

        ignored_tables = kwargs.get('ignored_tables',[])
        ignored_attributes = kwargs.get('ignored_attributes',[])

        #Input:  A keyword query Q=[k1, k2, . . . , km]
        #Output: Set of non-free and non-empty tuple-sets Rq

        '''
        The tuple-set Rki contains the tuples of Ri that contain all
        terms of K and no other keywords from Q
        '''

        #Part 1: Find sets of tuples containing each keyword
        P = {}
        logger.debug("Processing keyword values")
        for keyword in Q:
            if keyword not in value_index:
                continue

            for table in value_index[keyword]:
                if table in ignored_tables:
                    continue

                for (attribute,ctids) in value_index[keyword][table].items():
                    if (table,attribute) in ignored_attributes:
                        continue

                    vkm = KeywordMatch(table, value_filter={attribute:{keyword}})
                    P[vkm] = set(ctids)

        #Part 2: Find sets of tuples containing larger termsets
        self.tupleset_iterator(P, check_attributes=True)
        self.tupleset_iterator(P, check_attributes=False) 

        #Part 3: Ignore tuples
        return set(P)

    def tupleset_iterator(self, P, check_attributes=False):
        #Input: A Set of non-empty tuple-sets for each keyword alone P
        #Output: The Set P, but now including larger termsets (process Intersections)

        '''
        Termset is any non-empty subset K of the terms of a query Q
        '''

        for ( vkm_i , vkm_j ) in itertools.combinations(P,2):

            #print("Analysing: {} {} with {} elements in common".format(vkm_i, vkm_j, len(P[vkm_i] & P[vkm_j])))
            if (vkm_i.table == vkm_j.table and
                set(vkm_i.keywords()).isdisjoint(vkm_j.keywords())
               ) and ((check_attributes and vkm_i.has_same_attribute(vkm_j)) 
               or not check_attributes):
                #print("merging: {} {}".format(vkm_i, vkm_j))
                joint_tuples = P[vkm_i] & P[vkm_j]

                if len(joint_tuples)>0:

                    joint_predicates = {}

                    for attribute, keywords in vkm_i.value_filter:
                        joint_predicates.setdefault(attribute,set()).update(keywords)

                    for attribute, keywords in vkm_j.value_filter:
                        joint_predicates.setdefault(attribute,set()).update(keywords)

                    vkm_ij = KeywordMatch(vkm_i.table,value_filter=joint_predicates)
                    P[vkm_ij] = joint_tuples

                    P[vkm_i].difference_update(joint_tuples)
                    if len(P[vkm_i])==0:
                        del P[vkm_i]

                    P[vkm_j].difference_update(joint_tuples)
                    if len(P[vkm_j])==0:
                        del P[vkm_j]

                    return self.tupleset_iterator(P, check_attributes=check_attributes)
        return {}

    def schema_keyword_match_generator(self, Q, schema_index,**kwargs):
        ignored_tables = kwargs.get('ignored_tables',[])
        ignored_attributes = kwargs.get('ignored_attributes',[])
        threshold = kwargs.get('threshold',0.8)
        keyword_matches_to_ignore = kwargs.get('keyword_matches_to_ignore',set())

        S = set()
        logger.debug("Processing schema matches")
        for keyword in Q:
            for table in schema_index:
                if table in ignored_tables:
                    continue

                for attribute in ['*']+list(schema_index[table].keys()):

                    if attribute=='id' or  (table,attribute) in ignored_attributes:
                        continue
                    
                    attribute_variants = self.get_attribute_variants(attribute)
                    for variant in attribute_variants:
                        sim = self.similarities.word_similarity(keyword,table,variant)
                        #logger.debug("similiary : {} threshold: {}".format(sim, threshold))
                        if sim >= threshold:
                            logger.info(f"found a KWmatch for {keyword} in {table}.{attribute} with score: {sim}")
                            skm = KeywordMatch(table,schema_filter={attribute:{keyword}})
                            #print(skm)
                            if skm not in keyword_matches_to_ignore:
                                S.add(skm)
                            #print(S)
        return S

    def filter_kwmatches_by_segments(self, kw_matches, segments):
        kw_result_sets = set()
        for segment in segments:
            keywords = set([x for x in segment.replace('"', '').split(' ') if not x in stopwords()])
            for kw_match in kw_matches:
                all_keywords = set([x for x in kw_match.keywords()])
                if all_keywords == keywords:
                    kw_result_sets.add(kw_match)

        return kw_result_sets


    def get_attribute_variants(self, attribute):
        pattern = re.compile('[_,-]')
        return pattern.split(attribute)

