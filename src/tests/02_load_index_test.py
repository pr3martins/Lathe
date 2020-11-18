import sys
import os
from pprint import pprint as pp
from glob import glob
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler, BabelHash

config = ConfigHandler()
indexHandler = IndexHandler()

print(os.path.abspath(__file__))

indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename,load_method='sample')

print('Schema Index:\n')
pp(dict(indexHandler.schema_index))

print('\n\nValue Index (Sampling 15 elements):\n')
print(indexHandler.value_index)
