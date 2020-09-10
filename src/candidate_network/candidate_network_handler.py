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

        CN = CandidateNetwork()
        CN.add_vertex(query_match.get_random_keyword_match())

        if len(query_match)==1:
            if non_empty_only:
                sql = get_sql_from_cn(schema_graph,
                                         CN,
                                         rowslimit=1)

                non_empty = exec_sql(conn_string,
                                     sql,
                                     show_results=False)


            if non_empty_only and non_empty==False:
                return {}
            return {CN}

        returned_cns = list()
        ignored_cns = list()

        table_hash={}
        for keyword_match in query_match:
            table_hash.setdefault(keyword_match.table,set()).add(keyword_match)

        F = deque()
        F.append(CN)

        while F:
            CN = F.popleft()

            for vertex_u in CN.vertices():
                keyword_match,alias = vertex_u

                sorted_directed_neighbors = sorted(schema_graph.directed_neighbours(keyword_match.table),
                                                   reverse=True,
                                                   key=directed_neighbor_sorting_function)

                for direction,adj_table in sorted_directed_neighbors:

                    if adj_table in table_hash:
                        for adj_keyword_match in table_hash[adj_table]:

                            if adj_keyword_match not in CN.keyword_matches():

                                new_CN = deepcopy(CN)
                                vertex_v = new_CN.add_vertex(adj_keyword_match)
                                new_CN.add_edge(vertex_u,vertex_v,edge_direction=direction)

                                if (new_CN not in F and
                                    new_CN not in returned_cns and
                                    new_CN not in ignored_cns and
                                    len(new_CN)<=max_cn_size and
                                    new_CN.is_sound() and
                                    len(list(new_CN.leaves())) <= len(query_match) and
                                    new_CN.num_free_keyword_matches()+len(query_match) <= max_cn_size
                                   ):

                                    if new_CN.minimal_cover(query_match):

                                        if non_empty_only == False:

                                            current_time = timer()
                                            gns_elapsed_time.append(current_time-start_time)

                                            returned_cns.append(new_CN)

                                            if new_CN == desired_cn:
                                                return returned_cns
                                        else:
                                            sql = get_sql_from_cn(schema_graph,
                                                                     new_CN,
                                                                     rowslimit=1)

                                            non_empty = exec_sql(conn_string,
                                                                 sql,
                                                                 show_results=False)

                                            if non_empty:
                                                current_time = timer()
                                                gns_elapsed_time.append(current_time-start_time)

                                                returned_cns.append(new_CN)

                                                if new_CN == desired_cn:
                                                    return returned_cns
                                            else:
                                                ignored_cns.append(new_CN)


                                        if len(returned_cns)>=topk_cns_per_qm:
                                            return returned_cns

                                    elif len(new_CN)<max_cn_size:
                                        F.append(new_CN)


                    new_CN = deepcopy(CN)
                    adj_keyword_match = KeywordMatch(adj_table)
                    vertex_v = new_CN.add_vertex(adj_keyword_match)
                    new_CN.add_edge(vertex_u,vertex_v,edge_direction=direction)
                    if (new_CN not in F and
                        len(new_CN)<max_cn_size and
                        new_CN.is_sound() and
                        len(list(new_CN.leaves())) <= len(query_match) and
                        new_CN.num_free_keyword_matches()+len(query_match) <= max_cn_size
                       ):
                        F.append(new_CN)

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


            for i,Cn in enumerate(Cns):
                if(Cn not in generated_cns):
                    if num_cns_available<=0:
                        break

                    generated_cns.append(Cn)

                    #Dividindo score pelo tamanho da cn (SEGUNDA PARTE DO RANKING)
                    cn_score = qm_score/len(Cn)

                    un_ranked_cns.append( (Cn,cn_score) )

                    num_cns_available -=1

        #Ordena CNs pelo CnScore
        ranked_cns=sorted(un_ranked_cns,key=lambda x: x[1],reverse=True)

        return ranked_cns
