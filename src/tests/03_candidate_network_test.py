import sys
sys.path.append('../')



from utils import ConfigHandler, Similarity, Tokenizer
from index import IndexHandler
from query_match import QueryMatchHandler
from keyword_match import KeywordMatchHandler
from candidate_network import CandidateNetworkHandler

config = ConfigHandler()
index_handler = IndexHandler()

tokenizer = Tokenizer(tokenize_method = 'simple')
index_handler.load_indexes(
    config.value_index_filename,
    config.schema_index_filename
)
index_handler.create_schema_graph()
similarity = Similarity(index_handler.schema_index)
keyword_match_handler = KeywordMatchHandler(similarity)
query_match_handler = QueryMatchHandler()
candidate_network_handler = CandidateNetworkHandler()

keyword_query = 'indiana jones last crusade lost ark'
keywords =  set(tokenizer.keywords(keyword_query))

print("Generating schema matches")
sk_matches = keyword_match_handler.schema_keyword_match_generator(keywords, index_handler.schema_index)
print(sk_matches)

print("Generating values matches")
vk_matches = keyword_match_handler.value_keyword_match_generator(keywords, index_handler.value_index)
print(vk_matches)

kw_matches = sk_matches+vk_matches

print("Generating Query Matches")
query_matches = query_match_handler.generate_query_matches(keywords, kw_matches)

print("Ranking query matches")
ranked_query_matches = query_match_handler.rank_query_matches(query_matches,
    index_handler.value_index,
    index_handler.schema_index,
    similarity)

ranked_query_matches = ranked_query_matches[:1] 

for query_match in ranked_query_matches:
    print(f'Score: {query_match.total_score:.3e}\tQM: {query_match}')

schema_graph = index_handler.schema_graph

print('Generating and Ranking Candidate Networks')
ranked_cns = candidate_network_handler.generate_cns(index_handler.schema_index,schema_graph,ranked_query_matches)

print("Candidate Networks:")
for candidate_network in ranked_cns:
    print(f'Score: {candidate_network.score:.3e}\nCN:\n{candidate_network}')