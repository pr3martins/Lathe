import sys

sys.path.append('../')

from utils import ConfigHandler, last_path
from evaluation import EvaluationHandler
from candidate_network import CandidateNetwork

evaluation_handler = EvaluationHandler()
data = evaluation_handler.load_results_from_file(approach = 'prepruning')

for i,result in enumerate(data['results']):
    keyword_query = result['keyword_query']
    relevant_qm_position = data['evaluation']['query_matches']['relevant_positions'][i]
    relevant_cn_position = data['evaluation']['candidate_networks']['relevant_positions'][i]
    print(f'{i+1}a Keyword Query: {keyword_query}')
    print(f'Relevant QM at position {relevant_qm_position}')
    print(f'Relevant CN at position {relevant_cn_position}')
    for j,candidate_network in enumerate(result['candidate_networks']):        
        if j>=relevant_cn_position and relevant_cn_position!=-1:
            break

        print(CandidateNetwork.from_json_serializable(candidate_network),end='\n\n')
        

# for i,relevant_position in enumerate(data['evaluation']['candidate_networks']['relevant_positions']):
#     keyword_query = data['results'][i]['keyword_query']
#     print(f'{i+1}a Keyword Query: {keyword_query}\nRelevant CN at position {relevant_position}\n')
#     for j,candidate_network in enumerate(data['results'][i]['candidate_networks']):
#         print(CandidateNetwork.from_json_serializable(candidate_network),end='\n\n')
#         if j+1>=relevant_position and relevant_position!=-1:
#             break
#     print()