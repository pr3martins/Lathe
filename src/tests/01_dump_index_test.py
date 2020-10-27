import sys
import os
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler

config = ConfigHandler()
indexHandler = IndexHandler()
#indexHandler.create_index_file()
#print(os.path.abspath(__file__))
#valueStructure = ValueStructure(5, 10)
#indexHandler.create_histogram()
#indexHandler.create_indexes()
#indexHandler.dump_indexes(config.value_index_filename, config.schema_index_filename)

print(os.path.abspath(__file__))
indexHandler.create_indexes()
