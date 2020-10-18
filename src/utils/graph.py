class Graph:
    __slots__ = ['__graph_dict','__enable_edge_info','__edges_info']
    def __init__(self, graph_dict=None, has_edge_info=False):
        if graph_dict == None:
            graph_dict = {}
        self.__graph_dict = graph_dict

        self.__enable_edge_info = has_edge_info
        if has_edge_info:
            self.__edges_info = {}
        else:
            self.__edges_info = None

    def add_vertex(self, vertex):
        self.__graph_dict.setdefault(vertex, (set(),set()) )
        return vertex

    def add_outgoing_neighbour(self,vertex,neighbour):
        self.__graph_dict[vertex][0].add(neighbour)

    def add_incoming_neighbour(self,vertex,neighbour):
        self.__graph_dict[vertex][1].add(neighbour)

    def outgoing_neighbours(self,vertex):
        yield from self.__graph_dict[vertex][0]

    def incoming_neighbours(self,vertex):
        yield from self.__graph_dict[vertex][1]

    def neighbours(self,vertex):
        #This method does not use directed_neighbours to avoid generating tuples with directions
        yield from self.outgoing_neighbours(vertex)
        yield from self.incoming_neighbours(vertex)

    def directed_neighbours(self,vertex):
        #This method does not use directed_neighbours to avoid generating tuples with directions
        for outgoing_neighbour in self.outgoing_neighbours(vertex):
            yield ('>',outgoing_neighbour)
        for incoming_neighbour in self.incoming_neighbours(vertex):
            yield ('<',incoming_neighbour)

    def add_edge(self, vertex1, vertex2,edge_info = None, edge_direction='>'):
        if edge_direction=='>':
            self.add_outgoing_neighbour(vertex1,vertex2)
            self.add_incoming_neighbour(vertex2,vertex1)

            if self.__enable_edge_info:
                self.__edges_info[(vertex1, vertex2)] = edge_info
        elif edge_direction=='<':
            self.add_edge(vertex2, vertex1,edge_info = edge_info)
        else:
            raise SyntaxError('edge_direction must be > or <')

    def get_edge_info(self,vertex1,vertex2):
        if self.__enable_edge_info == False:
            return None
        return self.__edges_info[(vertex1, vertex2)]

    def vertices(self):
        return self.__graph_dict.keys()

    def edges(self):
            for vertex in self.vertices():
                for neighbour in self.outgoing_neighbours(vertex):
                    yield (vertex,)+ (neighbour,)

    def dfs_pair_iter(self, source_iter = None, root_predecessor = False):
        last_vertex_by_level=[]

        if source_iter is None:
            source_iter = self.leveled_dfs_iter()

        for direction,level,vertex in source_iter:
            if level < len(last_vertex_by_level):
                last_vertex_by_level[level] = vertex
            else:
                last_vertex_by_level.append(vertex)

            if level>0:
                prev_vertex = last_vertex_by_level[level-1]
                yield (prev_vertex,direction,vertex)
            elif root_predecessor:
                yield (None,'',vertex)


    def leveled_dfs_iter(self,start_vertex=None,visited = None, level=0, direction='',two_way_transversal=True):
        if len(self)>0:
            if start_vertex is None:
                start_vertex = self.get_starting_vertex()
            if visited is None:
                visited = set()
            visited.add(start_vertex)

            yield( (direction,level,start_vertex) )

            for neighbour in self.outgoing_neighbours(start_vertex):
                if neighbour not in visited:
                    yield from self.leveled_dfs_iter(neighbour,visited,
                                                     level=level+1,
                                                     direction='>',
                                                     two_way_transversal=two_way_transversal)

            # two_way_transversal indicates whether the DFS will expand through incoming neighbours
            if two_way_transversal:
                for neighbour in self.incoming_neighbours(start_vertex):
                    if neighbour not in visited:
                        yield from self.leveled_dfs_iter(neighbour,visited,
                                                         level=level+1,
                                                         direction='<',
                                                         two_way_transversal=two_way_transversal)


    def leaves(self):
        for vertice in self.vertices():
            if sum(1 for neighbour in self.neighbours(vertice)) == 1:
                yield vertice

    def get_starting_vertex(self):
        return next(iter(self.vertices()))

    def __repr__(self):
        return repr(self.__graph_dict)

    def __len__(self):
         return len(self.__graph_dict)

    def str_graph_dict(self):
        return str(self.__graph_dict)

    def str_edges_info(self):
        return str(self.__edges_info)
