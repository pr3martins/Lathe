from k2db.mapper import Mapper
from k2db.utils import ConfigHandler, truncate
from k2db.evaluation import EvaluationHandler

config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()

print('Choose a queryset from the list:\n')
for i,(name,path) in enumerate(queryset_configs):
    print(f'{i+1:02d} - {truncate(name)}')    
ans = int(input())
# ans = 1

_ , queryset_config_filepath = queryset_configs[ans-1]
config = ConfigHandler(reset=True,queryset_config_filepath=queryset_config_filepath)

mapper = Mapper()
mapper.load_queryset()

print('\nChoose a keyword query from the list:\n')
for i,item in enumerate(mapper.queryset):
    keyword_query = item['keyword_query']
    print(f'{i+1:02d} - {truncate(keyword_query)}')

ans = int(input())
ans = 49

keyword_query = mapper.queryset[ans-1]['keyword_query']

results_for_query = mapper.keyword_search(
    keyword_query
)

results = {
    "database":config.connection['database'],
    "queryset":config.queryset_filepath,
    "results":[results_for_query],
}


evaluation_handler = EvaluationHandler()
evaluation_handler.load_golden_standards()
evaluated_results = evaluation_handler.evaluate_results(results)

print(evaluated_results['evaluation'])