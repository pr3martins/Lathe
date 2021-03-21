import sys

sys.path.append('../')

from utils import ConfigHandler, Similarity, next_path
from mapper import Mapper
from evaluation import EvaluationHandler

approaches = ['standard','prepruning','pospruning']
color_labels = ['CN-Std','CN-Pre','CN-Pos']

metrics = ['mrr', 'p@1', 'p@2', 'p@3', 'p@4']

databases = {
    'imdb': {
        'querysets':['coffman_imdb_renamed','coffman_imdb_renamed_clear_intents'],
        
    },
    'mondial': {
        'querysets':['coffman_mondial','coffman_mondial_clear_intents'],        
    }
}

repeat = {
    'standard'  :10,
    'prepruning':1,
    'pospruning':1,
}

keyword_query = 'suriname ethnic_group'

queryset = 'coffman_mondial'

queryset_config_file = f'../../config/queryset_configs/{queryset}_config.json'
config = ConfigHandler(reset = True,queryset_config_file=queryset_config_file)
mapper = Mapper()
evaluation_handler = EvaluationHandler()

mapper.load_queryset()
evaluation_handler.load_golden_standards()

results = mapper.keyword_search(keyword_query,approach='standard')

from pprint import pprint as pp
print('RESULTS:\n')
pp(results)



# for approach in approaches:
#     prepruning = (approach == 'prepruning')
#     pospruning = (approach == 'pospruning')

#     for database in databases:

#         querysets = databases[database]['querysets']

#         observations = {}
#         for j,queryset in enumerate(querysets):
#             queryset_config_file = f'../../config/queryset_configs/{queryset}_config.json'
#             config = ConfigHandler(reset = True,queryset_config_file=queryset_config_file)
#             mapper = Mapper()
#             evaluation_handler = EvaluationHandler()
#             mapper.load_queryset()
            
#             if parallel_cn:
#                 mapper.load_spark(2)

#             evaluation_handler.load_golden_standards()

#             results_filename = next_path(f'{config.results_directory}{config.queryset_name}-{approach}-%03d.json')

#             print(f'Running queryset {config.queryset_name} with {approach} approach')

#             results = mapper.run_queryset(
#                 parallel_cn=parallel_cn,
#                 repeat = repeat[approach],
#                 prepruning=prepruning,
#                 pospruning=pospruning
#                 )
#             evaluation_handler.evaluate_results(results,results_filename=results_filename)
