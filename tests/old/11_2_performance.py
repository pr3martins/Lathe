import sys
from json import load
from pprint import pprint as pp

sys.path.append('../')

from utils import ConfigHandler, Similarity, last_path
from mapper import Mapper
from evaluation import EvaluationHandler

approaches = ['standard','prepruning','pospruning'][:1]
color_labels = ['CN-Std','CN-Pre','CN-Pos'][:1]

metrics = ['mrr'] + [f'p@{k+1}' for k in range(4)]

databases = {
    'imdb': {
        'querysets':['coffman_imdb_renamed','coffman_imdb_renamed_clear_intents'],
        'querysets_labels' : ['IMDb','IMDb-DI']
        
    },
    'mondial': {
        'querysets':['coffman_mondial','coffman_mondial_clear_intents'],
        'querysets_labels' : ['MOND','MOND-DI']
    }
}

repeat = 10

parallel_cn = False

def load_evaluations(databases,approaches):
    evaluation = {}
    for database in databases:

        querysets = databases[database]['querysets']
        querysets_labels = databases[database]['querysets_labels']

        observations = {}
        for j,queryset in enumerate(querysets):
            for i,approach in enumerate(approaches):
                queryset_config_filepath = f'../../config/queryset_configs/{queryset}_config.json'
                config = ConfigHandler(reset = True,queryset_config_filepath=queryset_config_filepath)
                results_filename = last_path(f'{config.results_directory}{config.queryset_name}-{approach}-%03d.json')
                # print(results_filename)

                evaluation.setdefault(database,{}).setdefault(queryset,{}).setdefault(approach,{})
                
                with open(results_filename,mode='r') as f:
                    evaluation[database][queryset][approach] = load(f)['evaluation']
    return evaluation

def query_matches_precision(evaluation):
    metrics = [f'p@{k+1}' for k in range(10)]
    labels = {
        'coffman_mondial':'MOND',
        'coffman_mondial_clear_intents':'MOND-DI',
        'coffman_imdb_renamed':'IMDb',
        'coffman_imdb_renamed_clear_intents':'IMDb-DI',
    }
    dashes_order = [(1,0),(2,2),(1,0),(2,2)]

    precision_data = [
        (
            labels[queryset],
            [
                evaluation[database][queryset]['standard']['query_matches'][metric]
                for metric in metrics
            ]
        )
        for database in evaluation
        for queryset in evaluation[database]
    ]

    x_data = range(1,10+1)
    for (label,y_data),dashes in zip(precision_data,dashes_order):
        plt.plot(
            x_data,
            y_data,
            linewidth = 3,
            label = label,
            dashes = dashes
        )

    plt.title('Query Matches (New plot)')
    plt.xticks(np.arange(1, 10+1, 1.0))
    plt.xlabel('Rank Position K')
    plt.ylabel('P@K')
    plt.legend()
    plt.savefig('qms-precision-at-k.pdf')  
    plt.show()


evaluation = load_evaluations(databases,approaches)
query_matches_precision(evaluation)