import sys
import os
from pprint import pprint as pp
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler

config = ConfigHandler()
indexHandler = IndexHandler()

print(os.path.abspath(__file__))
indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename, sample_size=15)

print('Schema Index:\n')
pp(indexHandler.schema_index)

print('\n\nValue Index (Sampling 15 elements):\n')
pp(indexHandler.value_index)
