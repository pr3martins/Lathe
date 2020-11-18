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

#query = ["authors", "papers", "conference", "vldb", "2002", "1995"]
#query = ["hopmepage", "h", "v", "jagadish"]
# query = ["references", "making", "database", "systems", "usable"]
query = ['title','harrison','ford','george','lucas']

kwHandler = KeywordMatchHandler(similarity)
print("Generating schema matches")
skm_matches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index)
# skm_matches = kwHandler.filter_kwmatches_by_segments(skm_matches, ['references', '"making database systems usable"'])
# print(skm_matches)
print("Generating values matches")
vkm_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)
# print("Filter by segments")
# vkm_matches = kwHandler.filter_kwmatches_by_segments(vkm_matches,  ['references', '"making database systems usable"'])
print(vkm_matches)
qm_handler = QueryMatchHandler()

print("Generating Query Matches")
qm_handler.generate_query_matches(query, skm_matches | vkm_matches)
print(skm_matches | vkm_matches)
print("Ranking query matches")
qm_handler.rank_query_matches(indexHandler.value_index, indexHandler.schema_index, similarity, log_score=False)

for query_match in qm_handler.query_matches[:10]:
    print(query_match, query_match.total_score)
