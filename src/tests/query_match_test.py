import sys
sys.path.append('../')

from utils import ConfigHandler, Similarity
from index import IndexHandler
from query_match import QueryMatchHandler
from keyword_match import KeywordMatchHandler
config = ConfigHandler()
indexHandler = IndexHandler()
indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename)
similarity = Similarity(indexHandler.schema_index)

query = ["continent", "europe"]

kwHandler = KeywordMatchHandler(similarity)
sk_matches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index)
kv_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)
query_match = QueryMatchHandler()
query_match.generate_query_matches(query, kv_matches | sk_matches)
query_match.rank_query_matches(indexHandler.value_index, indexHandler.schema_index, similarity)

print (query_match.query_matches)