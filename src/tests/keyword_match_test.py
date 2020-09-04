import sys
sys.path.append('../')

from utils import ConfigHandler, Similarity
from index import IndexHandler
from keyword_match import KeywordMatchHandler

config = ConfigHandler()
indexHandler = IndexHandler()
indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename)
simialrity = Similarity(indexHandler.schema_index)

query = ["continent"]

kwHandler = KeywordMatchHandler(simialrity)
skmatches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index)
print (skmatches)

query = ["europe"]
kv_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)
print (kv_matches)