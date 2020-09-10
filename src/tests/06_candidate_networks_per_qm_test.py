import sys
sys.path.append('../')

from utils import ConfigHandler, Similarity
from index import IndexHandler
from keyword_match import KeywordMatchHandler
from query_match import QueryMatchHandler
from candidate_network import CandidateNetworkHandler


config = ConfigHandler()
indexHandler = IndexHandler()
indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename)
similarity = Similarity(indexHandler.schema_index)

query = ["continent", "europe"]

kwHandler = KeywordMatchHandler(similarity)
skm_matches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index)
vkm_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)

qm_handler = QueryMatchHandler()
qm_handler.generate_query_matches(query, skm_matches | vkm_matches)
qm_handler.rank_query_matches(indexHandler.value_index, indexHandler.schema_index, similarity)

indexHandler.create_schema_graph()

query_match = qm_handler.query_matches[0]

cn_handler = CandidateNetworkHandler()
cns = cn_handler.generate_cns_per_qm(indexHandler.schema_index,indexHandler.schema_graph,query_match)

print(f'\nCandidate Networks for Query Match {query_match}:\n')
for cn in cns:
    from pprint import pprint as pp
    pp(cn)
    print('\n')
