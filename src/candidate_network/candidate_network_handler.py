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

        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(\
            self.config.connection['host'], self.config.connection['database'], \
            self.config.connection['user'], self.config.connection['password'])

    def generate_cns_per_qm(self,schema_index,schema_graph,query_match,**kwargs):

        show_log = kwargs.get('show_log',False)
        max_cn_size = kwargs.get('max_cn_size',5)
        topk_cns_per_qm = kwargs.get('topk_cns_per_qm',1)
        directed_neighbor_sorting_function = kwargs.get('directed_neighbor_sorting_function',
                                                        self.factory_sum_norm_attributes(schema_index))
        non_empty_only = kwargs.get('non_empty_only',False)
        desired_cn = kwargs.get('desired_cn',None)
        gns_elapsed_time = kwargs.get('gns_elapsed_time',[])

        start_time = timer()

        if show_log:
            print('================================================================================\nSINGLE CN')
            print('max_cn_size ',max_cn_size)
            print('FM')
            pp(query_match)

        prev_candidate_network = CandidateNetwork()
        prev_candidate_network.add_vertex(query_match.get_random_keyword_match())

        if len(query_match)==1:
            if non_empty_only:
                sql = get_sql_from_cn(schema_graph,
                                         prev_candidate_network,
                                         rowslimit=1)

                non_empty = exec_sql(conn_string,
                                     sql,
                                     show_results=False)


            if non_empty_only and non_empty==False:
                return {}
            return {prev_candidate_network}

        returned_cns = list()
        ignored_cns = list()

        table_hash={}
        for keyword_match in query_match:
            table_hash.setdefault(keyword_match.table,set()).add(keyword_match)

        F = deque()
        F.append(prev_candidate_network)

        while F:
            prev_candidate_network = F.popleft()

            for vertex_u in CN.vertices():
                keyword_match,alias = vertex_u

                sorted_directed_neighbors = sorted(schema_graph.directed_neighbours(keyword_match.table),
                                                   reverse=True,
                                                   key=directed_neighbor_sorting_function)

                for direction,adj_table in sorted_directed_neighbors:

                    if adj_table in table_hash:
                        for adj_keyword_match in table_hash[adj_table]:

                            if adj_keyword_match not in prev_candidate_network.keyword_matches():

                                cur_candidate_network = deepcopy(prev_candidate_network)
                                vertex_v = cur_candidate_network.add_vertex(adj_keyword_match)
                                cur_candidate_network.add_edge(vertex_u,vertex_v,edge_direction=direction)

                                if (cur_candidate_network not in F and
                                    cur_candidate_network not in returned_cns and
                                    cur_candidate_network not in ignored_cns and
                                    len(cur_candidate_network)<=max_cn_size and
                                    cur_candidate_network.is_sound() and
                                    len(list(cur_candidate_network.leaves())) <= len(query_match) and
                                    cur_candidate_network.num_free_keyword_matches()+len(query_match) <= max_cn_size
                                   ):

                                    if cur_candidate_network.minimal_cover(query_match):

                                        if non_empty_only == False:

                                            current_time = timer()
                                            gns_elapsed_time.append(current_time-start_time)

                                            returned_cns.append(cur_candidate_network)

                                            if cur_candidate_network == desired_cn:
                                                return returned_cns
                                        else:
                                            sql = get_sql_from_cn(schema_graph,
                                                                     cur_candidate_network,
                                                                     rowslimit=1)

                                            non_empty = exec_sql(conn_string,
                                                                 sql,
                                                                 show_results=False)

                                            if non_empty:
                                                current_time = timer()
                                                gns_elapsed_time.append(current_time-start_time)

                                                returned_cns.append(cur_candidate_network)

                                                if cur_candidate_network == desired_cn:
                                                    return returned_cns
                                            else:
                                                ignored_cns.append(cur_candidate_network)


                                        if len(returned_cns)>=topk_cns_per_qm:
                                            return returned_cns

                                    elif len(cur_candidate_network)<max_cn_size:
                                        F.append(cur_candidate_network)


                    cur_candidate_network = deepcopy(prev_candidate_network)
                    adj_keyword_match = KeywordMatch(adj_table)
                    vertex_v = cur_candidate_network.add_vertex(adj_keyword_match)
                    cur_candidate_network.add_edge(vertex_u,vertex_v,edge_direction=direction)
                    if (cur_candidate_network not in F and
                        len(cur_candidate_network)<max_cn_size and
                        cur_candidate_network.is_sound() and
                        len(list(cur_candidate_network.leaves())) <= len(query_match) and
                        cur_candidate_network.num_free_keyword_matches()+len(query_match) <= max_cn_size
                       ):
                        F.append(cur_candidate_network)

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


        un_ranked_cns = []
        generated_cns=[]

        num_cns_available = topk_cns

        for i,query_match in enumerate(ranked_query_matches):
            qm_score = query_match.total_score


            if topk_cns!=-1 and num_cns_available<=0:
                break

            if show_log:
                print('{}ª QM:\n{}\n'.format(i+1,query_match))
            Cns = self.generate_cns_per_qm(schema_index,schema_graph,query_match,**CNGraphGen_kwargs)
            if show_log:
                print('Cns:')
                pp(Cns)


            for i,candidate_network in enumerate(Cns):
                if(candidate_network not in generated_cns):
                    if num_cns_available<=0:
                        break

                    generated_cns.append(candidate_network)

                    #Dividindo score pelo tamanho da cn (SEGUNDA PARTE DO RANKING)
                    cn_score = qm_score/len(candidate_network)

                    un_ranked_cns.append( (candidate_network,cn_score) )

                    num_cns_available -=1

        #Ordena CNs pelo CnScore
        ranked_cns=sorted(un_ranked_cns,key=lambda x: x[1],reverse=True)

        return ranked_cns
