from queue import deque
from copy import deepcopy

from k2db.utils import ConfigHandler,get_logger
from k2db.keyword_match import KeywordMatch
from k2db.database import DatabaseHandler

from .candidate_network import CandidateNetwork

logger = get_logger(__name__)
class CandidateNetworkHandler:
    def __init__(self,**kwargs):
        self.config = ConfigHandler()
        self.database_handler = kwargs.get('database_handler',DatabaseHandler())


    def generate_cns(self,schema_index,schema_graph,ranked_query_matches,weight_scheme,**kwargs):
        topk_cns = kwargs.get('topk_cns',20)
        pospruning = kwargs.get('pospruning',False)

        returned_cns=[]

        num_cns_available = topk_cns

        for query_match in ranked_query_matches:
            if topk_cns!=-1 and num_cns_available<=0:
                break

            cns_per_cur_qm = self.generate_cns_per_qm(schema_index,schema_graph,query_match,weight_scheme,**kwargs)

            for candidate_network in cns_per_cur_qm:
                if num_cns_available<=0:
                    break

                if not pospruning or not self.is_cn_empty(schema_graph,candidate_network):
                    returned_cns.append(candidate_network)

                num_cns_available -=1

        ranked_cns=sorted(returned_cns,key=lambda candidate_network: candidate_network.score,reverse=True)

        return ranked_cns

    def parallelized_generate_cns(self, spark_context, schema_index, schema_graph,ranked_query_matches,weight_scheme, **kwargs):
        topk_cns = kwargs.get('topk_cns',20)
        pospruning = kwargs.get('pospruning',False)

        qms_rdd = spark_context.parallelize(ranked_query_matches)
        cns_rdd = qms_rdd.flatMap(lambda query_match: self.generate_cns_per_qm(schema_index,schema_graph,query_match,weight_scheme,**kwargs))
        if pospruning:
            cns_rdd = cns_rdd.filter(lambda candidate_network: self.is_cn_empty(schema_graph,candidate_network))
        ranked_cns = cns_rdd.takeOrdered(topk_cns, lambda candidate_network: -candidate_network.score)

        return ranked_cns

    def generate_cns_per_qm(self,schema_index,schema_graph,query_match,weight_scheme,**kwargs):
        max_cn_size = kwargs.get('max_cn_size',5)
        topk_cns_per_qm = kwargs.get('topk_cns_per_qm',1)

        directed_neighbor_sorting_function = kwargs.get('directed_neighbor_sorting_function',
                                                        self.factory_sum_norm_attributes(schema_index,weight_scheme))
        prepruning = kwargs.get('prepruning',False)
        schema_prunning = kwargs.get('schema_prunning',True)
        desired_cn = kwargs.get('desired_cn',None)

        returned_cns = set()
        pruned_cns = set()

        def meet_pruning_conditions(jnkm):
            return not (
                #corretude rules
                jnkm.is_sound(schema_graph) and
                #checking whether it was already generated
                jnkm not in F and
                jnkm not in pruned_cns and
                jnkm not in returned_cns and
                # max cn size pruning
                len(jnkm)<=max_cn_size and
                (
                    not schema_prunning or
                    (
                        # max number of leaves pruning
                        jnkm.num_leaves() <= len(query_match) and
                        # max number of keyword-free matches pruning
                        jnkm.num_free_keyword_matches()+len(query_match) <= max_cn_size
                    )
                )
            )

        # JNKM stands for Joining Network of Keyword Matches
        cur_jnkm = CandidateNetwork()
        cur_jnkm.add_keyword_match(query_match.get_random_keyword_match())

        if len(query_match)==1:
            cur_jnkm.calculate_score(query_match)
            returned_cns.add(cur_jnkm)
            return returned_cns

        table_hash={}
        for keyword_match in query_match:
            table_hash.setdefault(keyword_match.table,set()).add(keyword_match)

        F = deque()
        F.append(cur_jnkm)

        while F:
            cur_jnkm = F.popleft()

            for vertex_u in reversed(cur_jnkm.vertices()):
                keyword_match,_ = vertex_u

                # sorted_directed_neighbors = schema_graph.directed_neighbors(keyword_match.table)

                sorted_directed_neighbors = sorted(
                    schema_graph.directed_neighbors(keyword_match.table),
                    reverse=True,
                    key=directed_neighbor_sorting_function
                    )

                for direction,adj_table in sorted_directed_neighbors:

                    table_hash.setdefault(adj_table,set())
                    keyword_free_match = KeywordMatch(adj_table)
                    table_hash[adj_table].add(keyword_free_match)

                    for adj_keyword_match in table_hash[adj_table]:

                        if (adj_keyword_match not in cur_jnkm.keyword_matches() or
                            adj_keyword_match.is_free()):

                            next_jnkm = deepcopy(cur_jnkm)
                            next_jnkm.add_adjacent_keyword_match(vertex_u,adj_keyword_match,edge_direction=direction)

                            if not meet_pruning_conditions(next_jnkm):
                                # print(f'next_jnkm:\n{next_jnkm}')
                                if next_jnkm.is_total(query_match):
                                    if not next_jnkm.contains_keyword_free_match_leaf():
                                        if not prepruning or not self.is_cn_empty(schema_graph,next_jnkm):
                                            next_jnkm.calculate_score(query_match)
                                            returned_cns.add(next_jnkm)

                                            if desired_cn is not None and next_jnkm == desired_cn:
                                                return returned_cns
                                    else:
                                        #reduce JNKM
                                        # print(f'next_jnkm(pruned): non minimal\n{next_jnkm}')
                                        pruned_cns.add(next_jnkm)
                                        continue

                                    if len(returned_cns)>=topk_cns_per_qm:
                                        return returned_cns

                                elif len(next_jnkm)<max_cn_size:
                                    F.append(next_jnkm)
                                    # print(f'next_jnkm:\n{next_jnkm}')
                                else:
                                    pruned_cns.add(next_jnkm)
                            else:
                                pruned_cns.add(next_jnkm)                                
        return returned_cns

    def factory_sum_norm_attributes(self,schema_index,weight_scheme):

        def sum_norm_attributes(directed_neighbor):
            _,adj_table = directed_neighbor
            if adj_table not in schema_index:
                return 0
            return sum(schema_index[adj_table][attribute]['norm'][weight_scheme]
                        for attribute in schema_index[adj_table])

        return sum_norm_attributes
    
    def is_cn_empty(self,schema_graph,candidate_network):
        sql = candidate_network.get_sql_from_cn(schema_graph)
        return self.database_handler.exist_results(sql) == False