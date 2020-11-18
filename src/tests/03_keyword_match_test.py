import sys
sys.path.append('../')

from utils import ConfigHandler, Similarity
from index import IndexHandler
from keyword_match import KeywordMatchHandler

#query = ["papers", "making", "database", "systems", "usable"]
#query = ["authors", "papers", "conference", "vldb", "2002", "1995"]
#query = ["university", "homepage", "michingan"]
# query = ["references", "making", "database", "systems", "usable"]
query = ['title','harrison','ford','george','lucas']
keywords = query


config = ConfigHandler()
indexHandler = IndexHandler()
indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename,keywords=keywords)
similarity = Similarity(indexHandler.schema_index)


print(f'Testing SKMGen for query {query}')
kwHandler = KeywordMatchHandler(similarity)
skm_matches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index)
print (skm_matches)

print(f'Testing VKMGen for query {query}')
vkm_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)
print (vkm_matches | skm_matches)
