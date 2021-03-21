import sys

sys.path.append('../')

from utils import ConfigHandler, last_path
from evaluation import EvaluationHandler
from candidate_network import CandidateNetwork
from query_match import QueryMatch
from mapper import Mapper

querysets_hash = {
    'imdb'   :['coffman_imdb_renamed','coffman_imdb_renamed_clear_intents'],
    'mondial':['coffman_mondial','coffman_mondial_clear_intents'],
}
approach = 'standard'

queryset=[
    queryset
    for queryset_list in querysets_hash.values()
    for queryset in queryset_list 
    ][3]


queryset_config_file = f'../../config/queryset_configs/{queryset}_config.json'
config = ConfigHandler(reset = True,queryset_config_file=queryset_config_file)


mapper = Mapper()
from pprint import pprint as pp
pp(mapper.index_handler.schema_index._dict)

evaluation_handler = EvaluationHandler()
data = evaluation_handler.load_results_from_file()

for i,result in enumerate(data['results']):
    keyword_query = result['keyword_query']
    relevant_qm_position = data['evaluation']['query_matches']['relevant_positions'][i]
    relevant_cn_position = data['evaluation']['candidate_networks']['relevant_positions'][i]
    print(f'{i+1}a Keyword Query: {keyword_query}')
    print(f'Relevant QM at position {relevant_qm_position}')
    for j,query_match in enumerate(result['query_matches']):        
        if j>=relevant_qm_position and relevant_qm_position!=-1:
            break

        print(QueryMatch.from_json_serializable(query_match),end='\n\n')