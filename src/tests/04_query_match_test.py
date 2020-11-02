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

query = ["papers", "making", "database", "systems", "usable"]

kwHandler = KeywordMatchHandler(similarity)
skm_matches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index)
vkm_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)

qm_handler = QueryMatchHandler()
qm_handler.generate_query_matches(query, skm_matches | vkm_matches)
qm_handler.rank_query_matches(indexHandler.value_index, indexHandler.schema_index, similarity, log_score=False)

for query_match in qm_handler.query_matches:
    print(query_match, query_match.total_score)
