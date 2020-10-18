import json
from collections import Counter  #used to check whether two CandidateNetworks are equal

from utils import Graph

class CandidateNetwork(Graph):
    def add_vertex(self, vertex, default_alias=True):
        if default_alias:
            vertex = (vertex, 't{}'.format(self.__len__()+1))
        return super().add_vertex(vertex)

    def keyword_matches(self):
        return {keyword_match for keyword_match,alias in self.vertices()}

    def non_free_keyword_matches(self):
        return {keyword_match for keyword_match,alias in self.vertices() if not keyword_match.is_free()}

    def num_free_keyword_matches(self):
        i=0
        for keyword_match,alias in self.vertices():
            if keyword_match.is_free():
                i+=1
        return i

    def is_sound(self):
        if len(self) < 3:
            return True

        #check if there is a case A->B<-C, when A.table=C.table
        for vertex,(outgoing_neighbours,incoming_neighbours) in self._Graph__graph_dict.items():
            if len(outgoing_neighbours)>=2:
                outgoing_tables = set()
                for neighbour,alias in outgoing_neighbours:
                    if neighbour.table not in outgoing_tables:
                        outgoing_tables.add(neighbour.table)
                    else:
                        return False

        return True

    def get_starting_vertex(self):
        vertex = None
        for vertex in self.vertices():
            keyword_match,alias = vertex
            if not keyword_match.is_free():
                break
        return vertex

    def remove_vertex(self,vertex):
        print('vertex:\n{}\n_Graph__graph_dict\n{}'.format(vertex,self._Graph__graph_dict))
        outgoing_neighbours,incoming_neighbours = self._Graph__graph_dict[vertex]
        for neighbour in incoming_neighbours:
            self._Graph__graph_dict[neighbour][0].remove(vertex)
        self._Graph__graph_dict.pop(vertex)

    def minimal_cover(self,QM):
        if self.non_free_keyword_matches()!=set(QM):
            return False

        for vertex in self.vertices():
            keyword_match,alias = vertex
            if keyword_match.is_free():
                visited = {vertex}
                start_node = next(iter( self.vertices() - visited ))

                for x in self.leveled_dfs_iter(start_node,visited=visited):
                    #making sure that the DFS algorithm runs until the end of iteration
                    continue

                if visited == self.vertices():
                    return False
        return True

    def unaliased_edges(self):
        for (keyword_match,alias),(neighbour_keyword_match,neighbour_alias) in self.edges():
            yield (keyword_match,neighbour_keyword_match)

    def __eq__(self, other):
        return hash(self)==hash(other) and isinstance(other,CandidateNetwork)

    #Although this is a multable object, we made the hash function since it is not supposed to change after inserted in the list of generated cns
    def __hash__(self):
        return hash((frozenset(Counter(self.unaliased_edges()).items()),frozenset(self.keyword_matches())))

    def __repr__(self):
        if len(self)==0:
            return 'EmptyCN'
        print_string = ['\t'*level+direction+str(vertex[0])  for direction,level,vertex in self.leveled_dfs_iter()]
        return '\n'.join(print_string)

    def to_json_serializable(self):
        return [{'keyword_match':keyword_match.to_json_serializable(),
            'alias':alias,
            'outgoing_neighbours':[alias for (km,alias) in outgoing_neighbours],
            'incoming_neighbours':[alias for (km,alias) in incoming_neighbours]}
            for (keyword_match,alias),(outgoing_neighbours,incoming_neighbours) in self._Graph__graph_dict.items()]

    def to_json(self):
        return json.dumps(self.to_json_serializable())

    @staticmethod
    def from_str(str_cn):

        def parser_iter(str_cn):
            re_cn = re.compile('^(\t*)([><]?)(.*)',re.MULTILINE)
            for i,(space,direction,str_km) in enumerate(re_cn.findall(str_cn)):
                yield (direction,len(space), (KeywordMatch.from_str(str_km),'t{}'.format(i+1)))

        imported_cn = CandidateNetwork()

        prev_vertex = None
        for prev_vertex,direction,vertex in imported_cn.dfs_pair_iter(root_predecessor=True,source_iter=parser_iter(str_cn)):
            imported_cn.add_vertex(vertex,default_alias=False)
            if prev_vertex is not None:
                imported_cn.add_edge(prev_vertex,vertex,edge_direction=direction)

        return imported_cn

    def from_json_serializable(json_serializable_cn):
        alias_hash ={}
        edges=[]
        for vertex in json_serializable_cn:
            keyword_match = KeywordMatch.from_json_serializable(vertex['keyword_match'])
            alias_hash[vertex['alias']]=keyword_match

            for outgoing_neighbour in vertex['outgoing_neighbours']:
                edges.append( (vertex['alias'],outgoing_neighbour) )

        candidate_network = CandidateNetwork()
        for alias,keyword_match in alias_hash.items():
            candidate_network.add_vertex( (keyword_match,alias) , default_alias=False)
        for alias1, alias2 in edges:
            vertex1 = (alias_hash[alias1],alias1)
            vertex2 = (alias_hash[alias2],alias2)
            candidate_network.add_edge(vertex1,vertex2)
        return candidate_network

    def from_json(json_cn):
        return CandidateNetwork.from_json_serializable(json.loads(json_cn))

    # TODO make the method below work
    def get_sql_from_cn(G,Cn,**kwargs):
        show_evaluation_fields=kwargs.get('show_evaluation_fields',False)
        rows_limit=kwargs.get('rows_limit',1000)

        #Future feature
        multiple_edges=False

        hashtables = {} # used for disambiguation

        selected_attributes = set()
        filter_conditions = []
        disambiguation_conditions = []
        selected_tables = []

        tables__search_id = []
        relationships__search_id = []

        for prev_vertex,direction,vertex in Cn.dfs_pair_iter(root_predecessor=True):
            keyword_match, alias = vertex
            for type_km,_ ,attr,keywords in keyword_match.mappings():
                selected_attributes.add('{}.{}'.format(alias,attr))
                if type_km == 'v':
                    for keyword in keywords:
                        condition = 'CAST({}.{} AS VARCHAR) ILIKE \'%{}%\''.format(alias,attr,keyword.replace('\'','\'\'') )
                        filter_conditions.append(condition)

            hashtables.setdefault(keyword_match.table,[]).append(alias)

            if show_evaluation_fields:
                tables__search_id.append('{}.__search_id'.format(alias))

            if prev_vertex is None:
                selected_tables.append('{} {}'.format(keyword_match.table,alias))
            else:
                # After the second table, it starts to use the JOIN syntax
                prev_keyword_match,prev_alias = prev_vertex
                if direction == '>':
                    constraint_keyword_match,constraint_alias = prev_vertex
                    foreign_keyword_match,foreign_alias = vertex
                else:
                    constraint_keyword_match,constraint_alias = vertex
                    foreign_keyword_match,foreign_alias = prev_vertex

                edge_info = G.get_edge_info(constraint_keyword_match.table,
                                            foreign_keyword_match.table)

                for constraint in edge_info:
                    join_conditions = []
                    for (constraint_column,foreign_column) in edge_info[constraint]:
                        join_conditions.append('{}.{} = {}.{}'.format(constraint_alias,
                                                                      constraint_column,
                                                                      foreign_alias,
                                                                      foreign_column ))

                    selected_tables.append('JOIN {} {} ON {}'.format(keyword_match.table,
                                                                     alias,
                                                                     '\n\t\tAND '.join(join_conditions)))
                    if not multiple_edges:
                        break

                if show_evaluation_fields:
                    relationships__search_id.append('({}.__search_id, {}.__search_id)'.format(alias,prev_alias))


        for table,aliases in hashtables.items():
            for i in range(len(aliases)):
                for j in range(i+1,len(aliases)):
                    disambiguation_conditions.append('{}.ctid <> {}.ctid'.format(aliases[i],aliases[j]))

        if len(tables__search_id)>0:
            tables__search_id = ['({}) AS Tuples'.format(', '.join(tables__search_id))]
        if len(relationships__search_id)>0:
            relationships__search_id = ['({}) AS Relationships'.format(', '.join(relationships__search_id))]

        sql_text = '\nSELECT\n\t{}\nFROM\n\t{}\nWHERE\n\t{}\nLIMIT {};'.format(
            ',\n\t'.join( tables__search_id+relationships__search_id+list(selected_attributes) ),
            '\n\t'.join(selected_tables),
            '\n\tAND '.join( disambiguation_conditions+filter_conditions),
            rows_limit)
        return sql_text
