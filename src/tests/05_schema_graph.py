import sys
import os
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler

config = ConfigHandler()
indexHandler = IndexHandler()

print(os.path.abspath(__file__))
indexHandler.create_schema_graph()

from pprint import pprint as pp
print('Schema Graph:\n')
pp(indexHandler.schema_graph)

print('\n\n\nEdges Info:\n')
pp(eval(indexHandler.schema_graph.str_edges_info()))
