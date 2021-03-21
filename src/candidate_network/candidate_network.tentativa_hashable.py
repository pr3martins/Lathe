import json
from collections import Counter  #used to check whether two CandidateNetworks are equal
from queue import deque
from pprint import pprint as pp

from keyword_match import KeywordMatch
from utils import Graph

class CandidateNetwork(Graph):

    def __init__(self, graph_dict=None, has_edge_info=False):
        self.score = None

        self.__hashable = None
        self.__hashcode = hash(None)
        self.__root = None
        self.__level_by_alias = {}

        super().__init__(graph_dict,has_edge_info)

        if len(self)>0:
            for vertex in self.vertices():
                keyword_match,alias = vertex
                if not keyword_match.is_free():
                    self.__root = vertex
                    self.__level_by_alias[alias]=0

            self.update_hashable()

    def add_vertex(self, vertex):
        if self.__root is None:
            keyword_match,alias = vertex

            if keyword_match.is_free():
                raise ValueError('The root of a Candidate Network cannot be a Keyword-Free Match.')
            else:    
                self.__root = vertex
                self.__level_by_alias[alias]=0 

        return super().add_vertex(vertex)

    def add_keyword_match(self, heyword_match, **kwargs):
        
        alias = kwargs.get('alias', 't{}'.format(self.__len__()+1))
        
        vertex = (heyword_match, alias)
        
        if self.__root is None:
            self.__root = vertex
            self.__level_by_alias[alias]=0            

        return super().add_vertex(vertex)

    def add_adjacent_keyword_match(self,parent_vertex,keyword_match,edge_direction='>',**kwargs):

        child_vertex = self.add_keyword_match(keyword_match,**kwargs)
        self.add_edge(parent_vertex,child_vertex,edge_direction=edge_direction)

        parent_km,parent_alias    = parent_vertex
        keyword_match,alias = child_vertex
        
        self.__level_by_alias[alias] = self.__level_by_alias[parent_alias]+1

        self.incremental_update_hashable(parent_vertex,child_vertex)

    def incremental_update_hashable(self,parent_vertex,child_vertex):
        parent_km,parent_alias    = parent_vertex
        child_km,child_alias = child_vertex

        child_level = self.__level_by_alias[child_alias]
        
        if len(self.__hashable)<child_level+1:
            self.__hashable.append(set())

        self.__hashable[child_level].add( (child_km,frozenset()) )

        parent_level = self.__level_by_alias[parent_alias]

        parent_neighbors = Counter(
            keyword_match 
            for keyword_match,alias in self.neighbours(parent_vertex)
        )

        old_parent_children = None
        new_parent_children = None

        # print(f'\n\n\nparent_vertex:')
        # pp(parent_vertex)

        # print(f'\nchild_vertex:')
        # pp(child_vertex)

        # print('\nHashable:')
        # pp(self.__hashable)

        # print(f'\nParent_neighbors:')
        # pp(parent_neighbors)

        for keyword_match,frozenset_children in self.__hashable[parent_level]:
            if keyword_match != parent_km:
                continue
            # The parent neighbors are equivalent to the parent children plus:
            # - one for the parent of the parent
            # - one for the new child
            children = Counter(dict(frozenset_children))
            # print(f'\nChildren:')
            # pp(children)

            difference = sum( (parent_neighbors-children).values() )
            if parent_vertex != self.__root:
                difference-=1
            
            if difference==1:
                old_parent_children = frozenset_children

                children[child_km]+=1

                # print(f'\nNew Children:')
                # pp(children)
                new_parent_children = frozenset(children.items())
                break

        self.__hashable[parent_level].remove( (parent_km,old_parent_children) )
        self.__hashable[parent_level].add( (parent_km,new_parent_children) )

        # print('\nHashable:')
        # pp(self.__hashable)

        self.__hashcode = hash(tuple(frozenset(items) for items in self.__hashable))
            

    def get_possible_root(self):
        for vertex in self.vertices():
            keyword_match,alias = vertex
            if not keyword_match.is_free():
                return vertex
        raise ValueError('The root of a Candidate Network cannot be a Keyword-Free Match.')

    def store_hashable(self):
        if self.__root is None:
            self.__root = get_possible_root()
        
        self.__hashable = []
        self.__level_by_alias = {}

        self.__hashcode = create_hashable(
            self.__root,
            hashable = self.__hashable,
            level_by_alias = self.__level_by_alias,
        )    

    def create_hashable(self,root,**kwargs):
        hashable = kwargs.get('hashable',[])
        level_by_alias = kwargs.get('level_by_alias',{})
        hashcode = kwargs.get()

        level = 0       
        visited = set()

        queue = deque()
        queue.append( (level,root) )

        while queue:
            level,vertex = queue.popleft()
            keyword_match,alias = vertex

            level_by_alias[alias]=level

            children = Counter()
            visited.add(alias)
            
            for adj_vertex in self.neighbours(vertex):
                adj_km,adj_alias = adj_vertex

                if adj_alias in visited:
                    continue

                queue.append( (level+1,adj_vertex) )
                children[adj_km]+=1
                
            if len(hashable)<level+1:
                hashable.append(set())
            
            hashable[level].add( (keyword_match,frozenset(children.items())) )

        hashcode = hash(tuple(frozenset(items) for items in hashable))

        return hashcode

    def keyword_matches(self):
        # return {keyword_match for keyword_match,alias in self.vertices()}
        for keyword_match,alias in self.vertices():
            yield keyword_match

    def non_free_keyword_matches(self):
        # return {keyword_match for keyword_match,alias in self.vertices() if not keyword_match.is_free()}
        for keyword_match,alias in self.vertices():
            if not keyword_match.is_free():
                yield keyword_match

    def num_free_keyword_matches(self):
        i=0
        for keyword_match,alias in self.vertices():
            if keyword_match.is_free():
                i+=1
        return i

    def is_sound(self):
        if len(self) < 3:
            return True

        #check if there is a case A->B<-C, when A.table==C.table
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

    def is_total(self,query_match):
        return set(self.non_free_keyword_matches())==set(query_match)

    def contains_keyword_free_match_leaf(self):
        for vertex in self.leaves():
            keyword_match,alias = vertex
            if keyword_match.is_free():
                return True
        return False

    def minimal_cover(self,query_match):
        return self.is_total(query_match) and not self.contains_keyword_free_match_leaf()

    def unaliased_edges(self):
        for (keyword_match,alias),(neighbour_keyword_match,neighbour_alias) in self.edges():
            yield (keyword_match,neighbour_keyword_match)

    def calculate_score(self, query_match):
        # self.score = query_match.total_score*len(query_match)/len(self)
        self.score = query_match.total_score/len(self)

    def __eq__(self, other):
        if not isinstance(other,CandidateNetwork):
            return False
        
        self_km,self_alias = self.__root
        other_km,other_alias = other.__root

        if self_km!=other_km:

            common_root = None
            if not self.__root[0].is_free():
                common_root=self.__root


            print(f'a: {self.__root}\tb:{other.__root}')
            raise SyntaxError

        return hash(self)==hash(other)

    def __hash__(self):
        return self.__hashcode       

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
            candidate_network.add_vertex( (keyword_match,alias) )
        for alias1, alias2 in edges:
            vertex1 = (alias_hash[alias1],alias1)
            vertex2 = (alias_hash[alias2],alias2)
            candidate_network.add_edge(vertex1,vertex2)
        
        #important phase
        candidate_network.update_hashable()

        return candidate_network

    def from_json(json_cn):
        return CandidateNetwork.from_json_serializable(json.loads(json_cn))

    def get_sql_from_cn(self,schema_graph,**kwargs):
        rows_limit=kwargs.get('rows_limit',1000)
        show_evaluation_fields=kwargs.get('show_evaluation_fields',False)
        
        hashtables = {} # used for disambiguation

        selected_attributes = set()
        filter_conditions = []
        disambiguation_conditions = []
        selected_tables = []

        tables__search_id = []
        relationships__search_id = []

        for prev_vertex,direction,vertex in self.dfs_pair_iter(root_predecessor=True):
            keyword_match, alias = vertex
            for type_km, table ,attribute,keywords in keyword_match.mappings():
                selected_attributes.add(f'{alias}.{attribute}')
                if type_km == 'v':
                    for keyword in keywords:
                        sql_keyword = keyword.replace('\'','\'\'')
                        condition = f"CAST({alias}.{attribute} AS VARCHAR) ILIKE \'%{sql_keyword}%\'"
                        filter_conditions.append(condition)

            hashtables.setdefault(keyword_match.table,[]).append(alias)

            if show_evaluation_fields:
                tables__search_id.append(f'{alias}.__search_id')

            if prev_vertex is None:
                selected_tables.append(f'{keyword_match.table} {alias}')
            else:
                # After the second table, it starts to use the JOIN syntax
                prev_keyword_match,prev_alias = prev_vertex
                if direction == '>':
                    constraint_keyword_match,constraint_alias = prev_vertex
                    foreign_keyword_match,foreign_alias = vertex
                else:
                    constraint_keyword_match,constraint_alias = vertex
                    foreign_keyword_match,foreign_alias = prev_vertex

                edge_info = schema_graph.get_edge_info(constraint_keyword_match.table,
                                            foreign_keyword_match.table)

                for constraint in edge_info:
                    join_conditions = []
                    for (constraint_column,foreign_column) in edge_info[constraint]:
                        join_conditions.append(
                            f'{constraint_alias}.{constraint_column} = {foreign_alias}.{foreign_column}'
                        )
                    txt_join_conditions = '\n\t\tAND '.join(join_conditions)
                    selected_tables.append(f'JOIN {keyword_match.table} {alias} ON {txt_join_conditions}')
                if show_evaluation_fields:
                    relationships__search_id.append(f'({alias}.__search_id, {prev_alias}.__search_id)')

        for table,aliases in hashtables.items():
            for i in range(len(aliases)):
                for j in range(i+1,len(aliases)):
                    disambiguation_conditions.append(f'{aliases[i]}.ctid <> {aliases[j]}.ctid')

        if len(tables__search_id)>0:
            tables__search_id = [f"({', '.join(tables__search_id)}) AS Tuples"]
        if len(relationships__search_id)>0:
            txt_relationships = ', '.join(relationships__search_id)
            relationships__search_id = [f'({txt_relationships}) AS Relationships']

        sql_text = '\nSELECT\n\t{}\nFROM\n\t{}\nWHERE\n\t{}\nLIMIT {};'.format(
            ',\n\t'.join( tables__search_id+relationships__search_id+list(selected_attributes) ),
            '\n\t'.join(selected_tables),
            '\n\tAND '.join( disambiguation_conditions+filter_conditions),
            rows_limit)
        return sql_text
