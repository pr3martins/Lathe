from k2db.mapper import Mapper
from k2db.utils import ConfigHandler, truncate
from k2db.evaluation import EvaluationHandler
from k2db.query_match import QueryMatch
from k2db.candidate_network import CandidateNetwork

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

mapper = Mapper()
mapper.load_queryset()

print('\nChoose a keyword query from the list:\n')
for i,item in enumerate(mapper.queryset):
    keyword_query = item['keyword_query']
    print(f'{i+1:02d} - {truncate(keyword_query)}')

ans = int(input())
# ans = 45

topk_cns = 20
instance_based_pruning = False
max_database_accesses = 7
assume_golden_qms=False
desired_cn = False
topk_cns_per_qm = 2
max_num_query_matches = 5

keyword_query = mapper.queryset[ans-1]['keyword_query']


results_for_query = mapper.keyword_search(
    keyword_query,
    max_num_query_matches=max_num_query_matches,
    topk_cns_per_qm = topk_cns_per_qm,
    topk_cns=topk_cns,
    instance_based_pruning=instance_based_pruning,
    max_database_accesses =  max_database_accesses,
    assume_golden_qms=assume_golden_qms,
    desired_cn = desired_cn,
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

# print(evaluated_results['evaluation'])

query_matches = [QueryMatch.from_json_serializable(json_serializable_qm)
                                 for json_serializable_qm in results_for_query['query_matches']]

candidate_networks = [CandidateNetwork.from_json_serializable(json_serializable_cn)
                                 for json_serializable_cn in results_for_query['candidate_networks']]

for i,candidate_network in enumerate(candidate_networks):
    print(f'{i+1} CN:\n{candidate_network}')   