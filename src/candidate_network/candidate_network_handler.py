from timeit import default_timer as timer
from queue import deque
from copy import deepcopy

from utils import ConfigHandler,get_logger
from keyword_match import KeywordMatch

from .candidate_network import CandidateNetwork

logger = get_logger(__name__)
class CandidateNetworkHandler:
    def __init__(self):
        self.config = ConfigHandler()


    def generate_cns_per_qm(self,schema_index,schema_graph,query_match,**kwargs):
        max_cn_size = kwargs.get('max_cn_size',5)
        topk_cns_per_qm = kwargs.get('topk_cns_per_qm',1)
        directed_neighbor_sorting_function = kwargs.get('directed_neighbor_sorting_function',
                                                        self.factory_sum_norm_attributes(schema_index))
        non_empty_only = kwargs.get('non_empty_only',False)
        desired_cn = kwargs.get('desired_cn',None)
        gns_elapsed_time = kwargs.get('gns_elapsed_time',[])


        def meet_pruning_conditions(jnkm, prepruning = False):
            # The prepruning is not for JNKM but only for actual CNs
            # if prepruning and is_empty:
            #     # instance-based pre pruning
            #     return False

            return not (
                #corretude rules
                jnkm not in F and
                jnkm not in returned_cns and
                jnkm.is_sound() and
                # top k cns per qm pruning
                # top k cns pruning
                # implemented in other method
                # max number of leaves pruning
                jnkm.num_leaves() <= len(query_match) and
                # max cn size pruning
                len(jnkm)<=max_cn_size and
                # max number of keyword-free matches pruning
                jnkm.num_free_keyword_matches()+len(query_match) <= max_cn_size and
                #pruning extra information
                jnkm not in ignored_cns
            )

        start_time = timer()

        returned_cns = list()
        ignored_cns = list()

        # JNKM stands for Joining Network of Keyword Matches
        cur_jnkm = CandidateNetwork()
        cur_jnkm.add_vertex(query_match.get_random_keyword_match())

        if len(query_match)==1:
            returned_cns.append(cur_jnkm)
            return returned_cns

        table_hash={}
        for keyword_match in query_match:
            table_hash.setdefault(keyword_match.table,set()).add(keyword_match)

        F = deque()
        F.append(cur_jnkm)

        while F:
            cur_jnkm = F.popleft()

            for vertex_u in cur_jnkm.vertices():
                keyword_match,alias = vertex_u

                # sorted_directed_neighbors = schema_graph.directed_neighbours(keyword_match.table)

                sorted_directed_neighbors = sorted(schema_graph.directed_neighbours(keyword_match.table),
                                                   reverse=True,
                                                   key=directed_neighbor_sorting_function)

                for direction,adj_table in sorted_directed_neighbors:

                    table_hash.setdefault(adj_table,set())
                    keyword_free_match = KeywordMatch(adj_table)
                    table_hash[adj_table].add(keyword_free_match)

                    for adj_keyword_match in table_hash[adj_table]:

                        if (adj_keyword_match not in cur_jnkm.keyword_matches() or
                            adj_keyword_match.is_free()):

                            next_jnkm = deepcopy(cur_jnkm)
                            vertex_v = next_jnkm.add_vertex(adj_keyword_match)
                            next_jnkm.add_edge(vertex_u,vertex_v,edge_direction=direction)

                            if not meet_pruning_conditions(next_jnkm):
                                if next_jnkm.is_total(query_match):
                                    if next_jnkm.contains_keyword_free_match_leaf():
                                        current_time = timer()
                                        gns_elapsed_time.append(current_time-start_time)
                                        returned_cns.append(next_jnkm)
                                    else:
                                        #reduce JNKM
                                        continue

                                    if len(returned_cns)>=topk_cns_per_qm:
                                        return returned_cns

                                elif len(next_jnkm)<max_cn_size:
                                    F.append(next_jnkm)

        return returned_cns

    def factory_sum_norm_attributes(self,schema_index):

        def sum_norm_attributes(directed_neighbor):
            direction,adj_table = directed_neighbor
            if adj_table not in schema_index:
                return 0
            return sum(schema_index[adj_table][attribute]['norm']
                        for attribute in schema_index[adj_table])

        return sum_norm_attributes

    def generate_cns(self,schema_index,schema_graph,ranked_query_matches,**kwargs):
        topk_cns = kwargs.get('topk_cns',20)
        show_log = kwargs.get('show_log',False)
        CNGraphGen_kwargs = kwargs.get('CNGraphGen_kwargs',{})

        returned_cns=[]

        num_cns_available = topk_cns

        for i,query_match in enumerate(ranked_query_matches):
            qm_score = query_match.total_score

            if topk_cns!=-1 and num_cns_available<=0:
                break

            if show_log:
                print('{}ª QM:\n{}\n'.format(i+1,query_match))
            cns_per_cur_qm = self.generate_cns_per_qm(schema_index,schema_graph,query_match,**CNGraphGen_kwargs)
            if show_log:
                print(f'Cns: {generated_cns}')

            for i,candidate_network in enumerate(cns_per_cur_qm):
                if(candidate_network not in returned_cns):
                    if num_cns_available<=0:
                        break

                    #Dividindo score pelo tamanho da cn (SEGUNDA PARTE DO RANKING)
                    candidate_network.score = qm_score/len(candidate_network)
                    returned_cns.append(candidate_network)

                    num_cns_available -=1

        #Ordena CNs pelo CnScore
        ranked_cns=sorted(returned_cns,key=lambda candidate_network: candidate_network.score,reverse=True)

        return ranked_cns
