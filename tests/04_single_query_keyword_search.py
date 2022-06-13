import json

from pylathedb.lathe import Lathe
from pylathedb.utils import ConfigHandler, truncate
from pylathedb.evaluation import EvaluationHandler
from pylathedb.query_match import QueryMatch
from pylathedb.candidate_network import CandidateNetwork

config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()

print('Choose a queryset from the list:\n')
for i,(name,path) in enumerate(queryset_configs):
    print(f'{i+1:02d} - {truncate(name)}')    
ans = int(input())
# ans = 3

_ , queryset_config_filepath = queryset_configs[ans-1]
config = ConfigHandler(reset=True,queryset_config_filepath=queryset_config_filepath)

lathe = Lathe()
lathe.get_queryset()

print('\nChoose a keyword query from the list:\n')
for i,item in enumerate(lathe.get_queryset()):
    keyword_query = item['keyword_query']
    print(f'{i+1:02d} - {truncate(keyword_query)}')

ans = int(input())
# ans = 45

topk_cns = 5
instance_based_pruning = True
max_database_accesses = 9
assume_golden_qms=False
desired_cn = False
topk_cns_per_qm = 2
max_num_query_matches = 5
weight_scheme = 0

keyword_query = lathe.get_queryset()[ans-1]['keyword_query']
# keyword_query='kuwait saudi arabia'


results_for_query = lathe.keyword_search(
    keyword_query,
    max_num_query_matches=max_num_query_matches,
    topk_cns_per_qm = topk_cns_per_qm,
    topk_cns=topk_cns,
    instance_based_pruning=instance_based_pruning,
    max_database_accesses =  max_database_accesses,
    assume_golden_qms=assume_golden_qms,
    desired_cn = desired_cn,
    weight_scheme=weight_scheme,
)

results = {
    "database":config.connection['database'],
    "queryset":config.queryset_filepath,
    "results":[results_for_query],
}


evaluation_handler = EvaluationHandler()
evaluation_handler.load_golden_standards()
evaluated_results = evaluation_handler.evaluate_results(
    results,
    results_filename=f'{config.results_directory}single_query_keyword_search.json'
)

print(evaluated_results['evaluation'])

for i,json_serializable_qm in enumerate(results_for_query['query_matches']):
    query_match = QueryMatch.from_json_serializable(json_serializable_qm)
    json_qm = json.dumps(json_serializable_qm,indent=4)
    print(f'{i+1} QM:\n{query_match}')
    # print(f'JSON:\n{json_qm}\n')

for i,json_serializable_cn in enumerate(results_for_query['candidate_networks']):
    candidate_network = CandidateNetwork.from_json_serializable(json_serializable_cn)
    json_cn = json.dumps(json_serializable_cn,indent=4)
    sql_cn = candidate_network.get_sql_from_cn(lathe.index_handler.schema_graph)
    print(f'{i+1} CN:\n{candidate_network}')   
    # print(f'JSON:\n{json_cn}\n')
    print(f'SQL:\n{sql_cn}\n')