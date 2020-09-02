from .keyword_match import KeywordMatch
from utils import ConfigHandler,get_logger

logger = get_logger(__name__)
class KeywordMatchHandler:
    def __init__(self):
        self.config = ConfigHandler()

    def VKMGen(Q,value_index,**kwargs):
        
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
        TSInterMartins(P)

        #Part 3: Ignore tuples
        return set(P)

    def TSInterMartins(P):
        #Input: A Set of non-empty tuple-sets for each keyword alone P
        #Output: The Set P, but now including larger termsets (process Intersections)

        '''
        Termset is any non-empty subset K of the terms of a query Q
        '''

        for ( vkm_i , vkm_j ) in itertools.combinations(P,2):


            if (vkm_i.table == vkm_j.table and
                set(vkm_i.keywords()).isdisjoint(vkm_j.keywords())
               ):

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

                    return TSInterMartins(P)
        return {}

    def SKMGen(Q,schema_index,similarities,**kwargs):
        ignored_tables = kwargs.get('ignored_tables',[])
        ignored_attributes = kwargs.get('ignored_attributes',[])
        threshold = kwargs.get('threshold',1)
        keyword_matches_to_ignore = kwargs.get('keyword_matches_to_ignore',set())

        S = set()

        for keyword in Q:
            for table in schema_index:
                if table in ignored_tables:
                    continue

                for attribute in ['*']+list(schema_index[table].keys()):

                    if attribute=='id' or  (table,attribute) in ignored_attributes:
                        continue

                    sim = similarities.word_similarity(keyword,table,attribute)

                    if sim >= threshold:
                        skm = KeywordMatch(table,schema_filter={attribute:{keyword}})

                        if skm not in keyword_matches_to_ignore:
                            S.add(skm)

        return S
