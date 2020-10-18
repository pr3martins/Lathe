from utils import Graph

class SchemaGraph(Graph):
    def __init__(self, graph_dict=None):
        super().__init__(has_edge_info=True)

    def add_fk_constraint(self,constraint,table,column,foreign_table,foreign_column):
        self.add_vertex(table)
        self.add_vertex(foreign_table)

        edge_info = {}
        if (table,foreign_table) in self._Graph__edges_info:
            edge_info = self._Graph__edges_info[(table,foreign_table)]

        edge_info.setdefault(constraint, []).append((column, foreign_column))

        self.add_edge(table, foreign_table, edge_info)

    def __repr__(self):
        if len(self)==0:
            return 'EmptyGraph'
        print_string = ['\t'*level+direction+vertex for direction,level,vertex in self.leveled_dfs_iter()]
        return '\n'.join(print_string)
