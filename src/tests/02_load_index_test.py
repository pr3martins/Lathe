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

#Testando o upload com um dos partial indexes
#filename=glob(f'{config.value_index_filename}.part*')[1]
#print(f'filename {filename}')


indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename)

print('Schema Index:\n')
pp(indexHandler.schema_index)

print('\n\nValue Index (Sampling 15 elements):\n')
print(indexHandler.get_value_mappings("database"))