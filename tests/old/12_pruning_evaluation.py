import sys
from json import load

sys.path.append('../')

from utils import ConfigHandler, last_path
from mapper import Mapper
from evaluation import EvaluationHandler


import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def grouped_bar_plot(observations,n,color_labels,group_labels,observation_labels,**kwargs):
    m = len(color_labels)
    num_groups = len(group_labels)
    group_size = n//num_groups if num_groups > 0 else 0
    
    print(f'num_groups {num_groups} group_size {group_size} m {m}')
    
    
    title = kwargs.get('title','')
    ylabel = kwargs.get('ylabel','')
    filename = kwargs.get('filename',None)
    observation_margin = kwargs.get('observation_margin',0.05) 
    subgroup_margin = kwargs.get('subgroup_margin',0.25)
    group_margin = kwargs.get('group_margin',0.8)
    
    group_labels = ['\n\n'+label for label in group_labels]


    data = observations

    width  = 1/(m)

    last_observation = 0
    last_group = 0
    step = 0

    x = []
    group_x = []
    for i in range(n):
        last_observation+=step
        x.append(last_observation)    
        end_of_group = (i+1)%(group_size) == 0
        if end_of_group:
            step=1+group_margin

            middle_of_group = (last_observation+last_group)/2
            group_x.append(middle_of_group)           
            last_group=last_observation+step
        else:
            step=1+subgroup_margin

    x=np.array(x)
    group_x = np.array(group_x)

    fig, ax = plt.subplots(figsize = [15, 4.8])
    for i in range(m):
        print(data[i])
        print(color_labels[i])
        ax.bar(x+( (1-m)/2 + i)*width, data[i], width*(1-2*observation_margin), label=color_labels[i])

    ax.set_xticks(x)
    ax.set_xticklabels(observation_labels)

    ax.set_xticks(np.concatenate((x,group_x)))
    ax.set_xticklabels(observation_labels+group_labels)

    ax.set_ylim(ymin=0, ymax=1)

    lgd=ax.legend(bbox_to_anchor=(1.1, 1.00))

    if filename is not None:
        plt.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')

        
    return plt


approaches = ['standard','pospruning','prepruning']
metrics = ['mrr', 'p@1', 'p@2', 'p@3', 'p@4']

color_labels = ['CN-Std','CN-Pos','CN-Pre']

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
            with open(results_filename,mode='r') as f:
                data = load(f)['evaluation']['candidate_networks']
                
            observations_of_approach = [data[metric] for metric in metrics]
            observations.setdefault(color_labels[i],{})[querysets_labels[j]] = observations_of_approach

    group_labels = list(map(lambda name:name.upper(),metrics))
    observation_labels = querysets_labels*len(group_labels)

    observations = [
        [
            observations[approach][queryset][i]
            for i in range(len(group_labels))
            for queryset in observations[approach]
        ]
        for approach in color_labels 
    ]

    filename = f'{config.plots_directory}instance-based-pruning-evaluation-{database}.pdf'
    grouped_bar_plot(
        observations,
        len(observation_labels),
        color_labels,
        group_labels,
        observation_labels,
        filename=filename,
    )