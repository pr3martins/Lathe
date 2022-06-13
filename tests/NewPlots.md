---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.9.1
  kernelspec:
    display_name: pylathedb
    language: python
    name: pylathedb
---

## Load Evaluation Results

```python
from json import load
from pprint import pprint as pp
import matplotlib.pyplot as plt
import numpy as np
from copy import deepcopy
import re

from pylathedb.utils import ConfigHandler, Similarity, last_path
from pylathedb.lathe import Lathe
from pylathedb.evaluation import EvaluationHandler

def avg(x):
    return sum(x)/len(x)

def std(x):
    np_x = np.array(x)
    return np_x.std()/(len(np_x)**0.5)

def load_evaluation(approaches,subfolder='',show_not_found = True):
    config = ConfigHandler()
    queryset_configs = config.get_queryset_configs()
    queryset_configs.sort()
    
    evaluation = {}
    for approach in approaches:
        for qsconfig_name,qsconfig_filepath in queryset_configs:
            config = ConfigHandler(reset = True,queryset_config_filepath=qsconfig_filepath)
            config.results_directory += subfolder

            results_filename = f'{config.results_directory}{config.queryset_name}-{approach}.json'
            
#             results_filename = last_path(f'{config.results_directory}{config.queryset_name}-{approach}-%03d.json')
            try:
                with open(results_filename,mode='r') as f:
                    data = load(f)
            except FileNotFoundError:
                if show_not_found:
                    print(f'File {results_filename} not found.')
                continue
            evaluation.setdefault(data['database'],{}).setdefault(qsconfig_name,{}).setdefault(approach,{})
            evaluation[data['database']][qsconfig_name][approach] = data['evaluation']
    return evaluation

approaches = ['standard','ipruning']
quest_approaches = ['ipruning']
subfolder = 'tsvector_field_test789_merged/top20_cns/9db_accesses/1cns_per_qm/'
# subfolder = 'tsvector_field_test789_merged/top20_cns/9db_accesses/1cns_per_qm/'
# subfolder = 'ipruning_checks/1cns_per_qm/7db_accesses/'
# subfolder = 'nun_cns/'


### Executar paper freeze
# subfolder = 'qm_generation/ws0/'



golden_qms_subfolder = 'cns_from_golden_qms/'
golden_qms_approaches = ['standard','ipruning']

labels_hash = {
    #datasets
    'imdb_coffman_subset':'IMDb',
    'mondial_coffman':'Mondial',
    #querysets
    'coffman_imdb':'IMDb',
    'coffman_imdb_clear_intents':'IMDb-DI',
#     'coffman_imdb_renamed':'IMDb',
#     'coffman_imdb_renamed_clear_intents':'IMDb-DI',
    'coffman_mondial':'MONDIAL',
    'coffman_mondial_clear_intents':'MONDIAL-DI',
    #approaches
    'standard':'CN-Std',
    'pospruning':'CN-Pos',
    'prepruning':'CN-Pre',
    'ipruning':'CN-IP',
    #comparison with quest approaches
    ('QUEST','ipruning'):'LATHE',
    ('QUEST','standard'):'LATHE',
    #phases
    'km':'Keyword Match',
    'skm':'Schema-Keyword Match',
    'vkm':'Value-Keyword Match',
    'qm':'Query Match',
    'cn':'Candidate Network',
}

phases = ['km','qm','cn']

evaluation = load_evaluation(approaches,subfolder=subfolder)

assume_golden_qms_evaluation = load_evaluation(golden_qms_approaches,subfolder=golden_qms_subfolder)

```

```python
merged_evaluation = {}
for i in [1,2,7,8,9]:
    subfolder = f'tsvector_field_test789_merged/top20_cns/9db_accesses/{i}cns_per_qm/'
    print(f'{i} CNs per QM')
    evaluation = load_evaluation(approaches,subfolder=subfolder)
    for dataset in evaluation:
        merged_evaluation.setdefault(dataset,{})
        
        for queryset in evaluation[dataset]:
            merged_evaluation[dataset].setdefault(queryset,{})
            
            for approach in evaluation[dataset][queryset]:
                new_approach = f'{i}{approach}'
                merged_evaluation[dataset][queryset][new_approach] = evaluation[dataset][queryset][approach]

new_approaches = ['1standard', '2standard', '8standard','9standard','1ipruning','2ipruning']               
for approach in new_approaches:
    i = approach[0]
    root_label = labels_hash[approach[1:]][3:]
    pcn = 0 if root_label=='Std' else 9
    labels_hash[approach] = f'5/{i}/{pcn}'
# instance_pruning_plot(merged_evaluation,labels_hash,new_approaches)
```

## Groupbar Plot

```python
def grouped_bar_plot(observations,n,color_labels,group_labels,observation_labels,**kwargs):
    m = len(color_labels)
    num_groups = len(group_labels)
    group_size = n//num_groups if num_groups > 0 else 0    
    
    hide_group_label = kwargs.get('hide_group_label',False)
    title = kwargs.get('title','')
    ylabel = kwargs.get('ylabel','')
    xlabel = kwargs.get('xlabel','')
    filename = kwargs.get('filename',None)
    observation_margin = kwargs.get('observation_margin',0.05) 
    subgroup_margin = kwargs.get('subgroup_margin',0.25)
    group_margin = kwargs.get('group_margin',0.8)
    bbox_to_anchor=kwargs.get('bbox_to_anchor', (1.1, 1.00))
    ymin=kwargs.get('ymin',0)
    ymax=kwargs.get('ymax',1)
    figsize = kwargs.get('figsize',[15, 4.8])
    show_legend = kwargs.get('show_legend',True)
    
    
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
            
            if not hide_group_label:
                group_x.append(middle_of_group)           
            last_group=last_observation+step
        else:
            step=1+subgroup_margin

    x=np.array(x)
    group_x = np.array(group_x)   

    fig, ax = plt.subplots(figsize = figsize)
    for i in range(m):
        ax.bar(x+( (1-m)/2 + i)*width, data[i], width*(1-2*observation_margin), label=color_labels[i])

    ax.set_xticks(x)
    ax.set_xticklabels(observation_labels)

    if not hide_group_label:
        ax.set_xticks(np.concatenate((x,group_x)))
        ax.set_xticklabels(observation_labels+group_labels)

    ax.set_ylim(ymin=ymin, ymax=ymax)
    ax.set_ylabel(ylabel)
    
    if xlabel != '':
        ax.set_xlabel(xlabel)    
    
    
    if show_legend:
        legend=ax.legend(bbox_to_anchor=bbox_to_anchor)
        bbox_extra_artists = (legend,)
    else:
        bbox_extra_artists = None
    
    if filename is not None:
        plt.savefig(filename, bbox_extra_artists=bbox_extra_artists, bbox_inches='tight')
    
    if show_legend:
        colors=[]
        fig = plt.figure(figsize=(2, 1.25))
        patches = [
            mpatches.Patch(color=color, label=label)
            for label, color in zip(color_labels, colors)]
        fig.legend(patches, color_labels, loc='center', frameon=False)
        plt.show()

    return plt
```

## General Results Table

```python
subfolder = 'nun_pruned_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
```

```python
data = {
    'h1':[],
    'h2':[],
    'km':[],
    'qm':[],
    'cn':[],
    'cn_iterations':[],
}

cn_observations = {}
subfolder = 'nun_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
for database in evaluation:
    for queryset in evaluation[database]:
        observations = evaluation[database][queryset]['standard']
        label = labels_hash[queryset]
        data['h1']+=[label,label]
        data['h2']+=['max','avg']
        data['km']+= [max(observations['num_keyword_matches']),avg(observations['num_keyword_matches'])]
        data['qm']+= [max(observations['num_query_matches']),avg(observations['num_query_matches'])]
        data['cn']+= [max(observations['num_candidate_networks']),avg(observations['num_candidate_networks'])]
        cn_observations[queryset]=np.array(observations['num_candidate_networks'])

subfolder = 'nun_pruned_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)

for database in evaluation:
    for queryset in evaluation[database]:
        pruned_cn_observations = np.array(evaluation[database][queryset]['standard']['num_candidate_networks'])
        cn_iterations = cn_observations[queryset] + pruned_cn_observations
        data['cn_iterations'] += [max(cn_iterations),avg(cn_iterations)]
        
```

## Num. Keyword Matches

```python
def plot_num_keyword_matches(evaluation,labels_hash):  
    config = ConfigHandler()
    
    data = []
    labels = []
    
    margin = 0.05
    xlim = [10**(0-margin), 10**(2+margin)]
    
    fig, ax = plt.subplots()
    ax.invert_yaxis()
    
    for database in evaluation:
        for queryset in evaluation[database]:
            cur_data = evaluation[database][queryset]['standard']['num_keyword_matches']
            data.append(cur_data)
            labels.append(labels_hash[queryset])
          
    plt.boxplot(data,labels=labels,vert=0,patch_artist=True)
    plt.xscale('symlog')    
    plt.xlim(xlim)
    
    plt.savefig(f'{config.plots_directory}num-keyword-matches.pdf')  
    plt.show()

plot_num_keyword_matches(evaluation,labels_hash)
```

## Num. Query Matches

```python
def plot_num_query_matches(evaluation,labels_hash): 
    config = ConfigHandler()    
    data = []
    labels = []
    
    margin = 0.05
    xlim = [-2*margin, 10**(3+margin)]
    xlim = [10**(0-margin), 10**(3+margin)]
    
    fig, ax = plt.subplots()
    ax.invert_yaxis()
    
    for database in evaluation:
        for queryset in evaluation[database]:
            cur_data = evaluation[database][queryset]['standard']['num_query_matches']
            data.append(cur_data)
            labels.append(labels_hash[queryset])
          
    plt.boxplot(data,labels=labels,vert=0,patch_artist=True)
    plt.xscale('symlog')
        
    plt.xlim(xlim)
    plt.savefig(f'{config.plots_directory}num-query-matches.pdf')  
    plt.show()

plot_num_query_matches(evaluation,labels_hash)
```

## Num. Candidate Networkd

```python
def plot_num_candidate_networks(evaluation,labels_hash,starting_index = 0,ending_index = 3): 
    config = ConfigHandler()    
    data = []
    labels = []
    
    margin = 0.05
#     xlim = [-2*margin, 10**(3+margin)]
    
    xlim = [10**(starting_index-margin), 10**(ending_index+margin)]
    
    fig, ax = plt.subplots()
    ax.invert_yaxis()
    
    for database in evaluation:
        for queryset in evaluation[database]:
            cur_data = evaluation[database][queryset]['standard']['num_candidate_networks']
            data.append(cur_data)
            labels.append(labels_hash[queryset])
          
    plt.boxplot(data,labels=labels,vert=0,patch_artist=True)
    plt.xscale('symlog')
        
    plt.xlim(xlim)
    plt.savefig(f'{config.plots_directory}num-candidate-networks.pdf')  
    plt.show()

plot_num_candidate_networks(evaluation,labels_hash)
```

### Normal

```python
subfolder = 'nun_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_num_candidate_networks(evaluation,labels_hash,starting_index = 0,ending_index = 3)
```

## Pruned CNs

```python
subfolder = 'nun_pruned_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_num_candidate_networks(evaluation,labels_hash,starting_index = 0,ending_index = 5)
```

## Num CNs/Num QMs

```python
def plot_num_cns_per_qm(evaluation,labels_hash,starting_index = 0,ending_index = 3): 
    
    def ceildiv(a, b):
        return -(-a // b)
    
    config = ConfigHandler()    
    data = []
    labels = []
    
    margin = 0.05
#     xlim = [-2*margin, 10**(3+margin)]
    xlim = [10**(starting_index-margin), 10**(ending_index+margin)]
    
    fig, ax = plt.subplots()
    ax.invert_yaxis()
    
    for database in evaluation:
        for queryset in evaluation[database]:
            cn_data = evaluation[database][queryset]['standard']['num_candidate_networks']
            qm_data = evaluation[database][queryset]['standard']['num_query_matches']
            
            cn_per_qm_data = [ceildiv(num_cn,num_qm) for num_cn,num_qm in zip(cn_data,qm_data)]
            cur_data = cn_per_qm_data
            data.append(cur_data)
            
            labels.append(labels_hash[queryset])
          
    plt.boxplot(data,labels=labels,vert=0,patch_artist=True)
#     plt.xscale('symlog')   
#     plt.xlim(xlim)
    plt.savefig(f'{config.plots_directory}num-candidate-networks.pdf')  
    plt.show()

plot_num_cns_per_qm(evaluation,labels_hash,starting_index = 0,ending_index = 4)
```

```python
subfolder = 'nun_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_num_cns_per_qm(evaluation,labels_hash,starting_index = 0,ending_index = 1)
```

```python
subfolder = 'nun_pruned_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_num_cns_per_qm(evaluation,labels_hash,starting_index = 0,ending_index = 4)
```

```python
list(zip([1,2,3,4],[5,6,7,8]))
```

```python
subfolder = 'nun_pruned_cns/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_num_candidate_networks(evaluation,labels_hash)
```

## Query Matches Max relevant position

```python
def max_relevant_qm_position(labels_hash,subfolder='qm_generation/',show_not_found=False): 
    original_subfolder = subfolder 
    results = {}
    for i in range(4):
        weight_scheme = f'ws{i}'
        subfolder = f'{original_subfolder}{weight_scheme}/'
        local_evaluation = load_evaluation(approaches,subfolder=subfolder,show_not_found=show_not_found)    

        for database in local_evaluation:
            for queryset in local_evaluation[database]:
                max_relevant_position = max(local_evaluation[database][queryset]['ipruning']['query_matches']['relevant_positions'])
                results.setdefault(labels_hash[queryset],[])
                results[labels_hash[queryset]].append( (max_relevant_position) )
    return results

max_relevant_qm_position(labels_hash,subfolder='qm_generation/')
```

## Query Match Ranking

```python
evaluation
```

```python
def query_matches_precision_plot(evaluation,labels_hash):
    config = ConfigHandler()
    metrics = [f'p@{k+1}' for k in range(10)]
    dashes_order = [(1,0),(2,2),(1,0),(2,2)]

    precision_data = [
        (
            labels_hash[queryset],
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

    plt.xticks(np.arange(1, 10+1, 1.0))
    plt.xlabel('Rank Position K')
    plt.ylabel('P@K')
    plt.legend()
    plt.savefig(f'{config.plots_directory}qms-precision-at-k.pdf')  
    plt.show()

query_matches_precision_plot(evaluation,labels_hash)
```

```python
def query_matches_mrr_plot(evaluation,labels_hash,**kwargs):
    config=ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(5)]
    group_labels = ['group']

    observations = [
                [
                    evaluation[database][queryset]['standard']['query_matches'][metric]
                    for metric in metrics
                ]
                for database in evaluation
                for queryset in evaluation[database]   
            ]

    
    
    observation_labels = [metric.upper() for metric in metrics]

    color_labels = [labels_hash[queryset] 
                    for database in evaluation 
                    for queryset in evaluation[database]]

    filename = kwargs.get('filename',f'{config.plots_directory}qm_ranking.pdf')
    grouped_bar_plot(
        observations,
        len(observation_labels),
        color_labels,
        group_labels,
        observation_labels,
        filename=filename,
        hide_group_label=True,
        bbox_to_anchor=(1.11,1)
    )
    
    
query_matches_mrr_plot(evaluation,labels_hash)
```

```python
def query_matches_mrr_plot(evaluation,labels_hash,**kwargs):
    config=ConfigHandler()
    color_labels = [
        labels_hash[queryset] 
        for database in evaluation 
        for queryset in evaluation[database]
    ]
    data = [
        evaluation[database][queryset]['standard']['query_matches']['mrr']
        for database in evaluation
        for queryset in evaluation[database]
    ]
    
    pp(data)

    fig, ax = plt.subplots(figsize = [6,3])

    ax.barh(
        range(len(data)),
        data,
        capsize=10,
        fill=True,
        tick_label=color_labels,
        color= ['C0','C1','C2','C3']
    )

    ax.invert_yaxis()
    ax.xaxis.grid(True)
    
    filename = f'{config.plots_directory}qm_ranking_mrr.pdf'
    
    plt.savefig(filename)
    plt.show()

    # ax.set_yticklabels(color_labels, fontsize=12)
query_matches_mrr_plot(evaluation,labels_hash)
```

```python
labels_hash
```

```python
def query_matches_mrr_plot(evaluation,labels_hash,**kwargs):
    config=ConfigHandler()
    color_labels = [
        labels_hash[queryset] 
        for database in evaluation 
        for queryset in evaluation[database]
    ]
    data = [
        evaluation[database][queryset]['standard']['query_matches']['mrr']
        for database in evaluation
        for queryset in evaluation[database]
    ]
    
    pp(data)

    fig, ax = plt.subplots(figsize = [6,3])

    ax.barh(
        range(len(data)),
        data,
        capsize=10,
        fill=True,
        tick_label=color_labels,
        color= ['C0','C1','C2','C3']
    )

    ax.invert_yaxis()
    ax.xaxis.grid(True)
    
    filename = f'{config.plots_directory}qm_ranking_mrr.pdf'
    
    plt.savefig(filename)
    plt.show()

    # ax.set_yticklabels(color_labels, fontsize=12)
query_matches_mrr_plot(evaluation,labels_hash)
```

```python
# def query_matches_mrr_plot(evaluation,labels_hash,**kwargs):
#     config=ConfigHandler()
#     group_labels = ['group']

#     fig, ax = plt.subplots(figsize = [6,2])
    
#     for database in evaluation:
#         for queryset in evaluation[database]:

#     for y,(label,(mean,std_err)) in enumerate(data[experiment].items()):
#         ax.barh(y,
#                mean,
#                height=0.6,
#                xerr=std_err,
#                ecolor='black',
#                capsize=10,
#                fill=True
#           )


#     ax.set_xlabel(experiment, fontsize=14)
#     ax.set_yticks(range(len(data[experiment])))
#     ax.set_yticklabels(data[experiment], fontsize=12)

# #         ax.set_title('Comparison with other approaches')
#     ax.xaxis.grid(True)

#     plt.tight_layout()
#     experiment_name = 'precision' if experiment == 'P@1' else 'recall'
#     filename = f'{config.plots_directory}comparison-with-QUEST-{experiment_name}-35queries.pdf'
#     plt.savefig(filename)
#     plt.show()
    
    
#     observations = [
#                 evaluation[database][queryset]['standard']['query_matches'][['mrr']]
#                 for database in evaluation
#                 for queryset in evaluation[database]   
#     ]

    
    
#     observation_labels = [metric.upper() for metric in metrics]

#     color_labels = [labels_hash[queryset] 
#                     for database in evaluation 
#                     for queryset in evaluation[database]]

#     filename = kwargs.get('filename',f'{config.plots_directory}qm_ranking.pdf')
#     grouped_bar_plot(
#         observations,
#         len(observation_labels),
#         color_labels,
#         group_labels,
#         observation_labels,
#         filename=filename,
#         hide_group_label=True,
#         bbox_to_anchor=(1.11,1)
#     )
    
    
# query_matches_mrr_plot(evaluation,labels_hash)
```

```python
#  for experiment in data:
# #         fig, ax = plt.subplots(figsize = [len(data['P@1'])*1.1, 4.8])
#         fig, ax = plt.subplots(figsize = [6,len(data['P@1'])*1.1])

#         for y,(label,(mean,std_err)) in enumerate(data[experiment].items()):
#             ax.barh(y,
#                    mean,
#                    height=0.6,
#                    xerr=std_err,
#                    ecolor='black',
#                    capsize=10,
#                    fill=True
#               )


#         ax.set_xlabel(experiment, fontsize=14)
#         ax.set_yticks(range(len(data[experiment])))
#         ax.set_yticklabels(data[experiment], fontsize=12)

# #         ax.set_title('Comparison with other approaches')
#         ax.xaxis.grid(True)

#         plt.tight_layout()
#         experiment_name = 'precision' if experiment == 'P@1' else 'recall'
#         filename = f'{config.plots_directory}comparison-with-QUEST-{experiment_name}-35queries.pdf'
#         plt.savefig(filename)
#         plt.show()
```

## Candidate Network Ranking

```python
# from json import load
# import matplotlib
# import matplotlib.pyplot as plt
# import numpy as np

# from pylathedb.utils import ConfigHandler, last_path
# from pylathedb.lathe import Lathe
# from pylathedb.evaluation import EvaluationHandler

# config = ConfigHandler()
```

### Standard CN Ranking

```python
def cn_standard_ranking(evaluation,labels_hash,**kwargs):
    config=ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(5)]
    group_labels = ['group']

    observations = [
                [
                    evaluation[database][queryset]['standard']['candidate_networks'][metric]
                    for metric in metrics
                ]
                for database in evaluation
                for queryset in evaluation[database]   
            ]

    
    
    observation_labels = [metric.upper() for metric in metrics]

    color_labels = [labels_hash[queryset] 
                    for database in evaluation 
                    for queryset in evaluation[database]]

    filename = kwargs.get('filename',f'{config.plots_directory}cn_ranking.pdf')
    grouped_bar_plot(
        observations,
        len(observation_labels),
        color_labels,
        group_labels,
        observation_labels,
        filename=filename,
        hide_group_label=True,
        bbox_to_anchor=(1.11,1)
    )
    
#     print(observations)
    
cn_standard_ranking(evaluation,labels_hash)
```

### Instance Pruning CN Ranking

```python
def instance_pruning_plot(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(10)]
    group_labels = [name.upper() for name in metrics]

    data = [
        (
            database,
            [
                (
                    approach,
                    [
                        evaluation[database][queryset][approach]['candidate_networks'][metric]
                        for metric in metrics
                        for queryset in evaluation[database]   
                    ],
                )
                for approach in approaches
            ]
        )
        for database in evaluation
    ]
    
    for database,approach_results in data:
        observations = [results for approach,results in approach_results]       

        observation_labels = [
            labels_hash[queryset]
            for group in group_labels
            for queryset in evaluation[database]
        ]
        
#         pp(observations)
#         print('-------------')
#         pp(observation_labels)
#         print('###########')
        
        filename = f'{config.plots_directory}instance-based-pruning-evaluation-{database}.pdf'
        grouped_bar_plot(
            observations,
            len(observation_labels),
            [labels_hash[approach] for approach in approaches],
            group_labels,
            observation_labels,
            filename=filename,
        )
        
    plt.show()
        
#         print(observations)
# instance_pruning_plot(evaluation,labels_hash,approaches)
```

```python
instance_pruning_plot(merged_evaluation,labels_hash,new_approaches)
```

#### Testando instance based pruning separando -DI

```python
merged_evaluation['imdb_coffman_subset'].keys()
```

```python
def instance_pruning_plot(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(10)]
    group_labels = ['group']

    data = [
        (
        clear_intent,
        [
            (
            database,
            [
                (
                    approach,
                    [
                        evaluation[database][queryset][approach]['candidate_networks'][metric]
                        for metric in metrics
                        for queryset in evaluation[database]
                        if ('_clear_intents' in queryset) == (clear_intent)
                    ],
                )
                for approach in approaches
            ]
            )
            for database in evaluation
        ]
        )
        for clear_intent in [False,True]
    ]
    print(f'data={data}')
    
    for clear_intent,database_results in data:
        for database,approach_results in database_results:
            observations = [results for approach,results in approach_results]       

            observation_labels = [metric.upper() for metric in metrics]

#             pp(observations)
#             print('-------------')
#             pp(observation_labels)
#             print('###########')

            title = labels_hash[database] + ('-DI' if clear_intent else '')
            print(title)
            filename = f'{config.plots_directory}instance-based-pruning-evaluation-queryset-{title}.pdf'
            grouped_bar_plot(
                observations,
                len(observation_labels),
                [labels_hash[approach] for approach in approaches],
                group_labels,
                observation_labels,
                filename=filename,
                hide_group_label=True,
#                 xlabel=title,
            )
        
    plt.show()
    
instance_pruning_plot(merged_evaluation,labels_hash,new_approaches)
```

```python
new_approaches
```

### Testing P@k correto

```python
int('1')
```

```python
def instance_pruning_plot(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(10)]
    group_labels = ['group']

    
    def evaluate_metric(metric,relevant_positions):
        if metric == 'mrr':
            return get_mean_reciprocal_rank(relevant_positions)
        else:
            match = re.match(r'p@(\d+)',metric)
            if match:
                k = int(match.groups()[0])
                return get_precision_at_k(k,relevant_positions)
            return 0
            
    
    def get_mean_reciprocal_rank(relevant_positions):
        if len(relevant_positions)==0:
            return 0
        sum = 0
        for relevant_position in relevant_positions:
            if relevant_position != -1:
                reciprocal_rank = 1/relevant_position
            else:
                reciprocal_rank = 0
            sum += reciprocal_rank

        mrr = sum/len(relevant_positions)
        return mrr
    
    def get_precision_at_k(k,relevant_positions):
        if len(relevant_positions)==0:
            return 0
        sum = 0
        for relevant_position in relevant_positions:
            if relevant_position <= k and relevant_position!=-1:
                sum+=1/k
        presition_at_k = sum/len(relevant_positions)
        return presition_at_k
    
    data = [
        (
        clear_intent,
        [
            (
            database,
            [
                (
                    approach,
                    [
                        evaluate_metric(metric,evaluation[database][queryset][approach]['candidate_networks']['relevant_positions'])
                        for metric in metrics
                        for queryset in evaluation[database]
                        if ('_clear_intents' in queryset) == (clear_intent)
                    ],
                )
                for approach in approaches
            ]
            )
            for database in evaluation
        ]
        )
        for clear_intent in [False,True]
    ]
    
    for clear_intent,database_results in data:
        for database,approach_results in database_results:
            observations = [results for approach,results in approach_results]       

            observation_labels = [metric.upper() for metric in metrics]

#             pp(observations)
#             print('-------------')
#             pp(observation_labels)
#             print('###########')

            title = labels_hash[database] + ('-DI' if clear_intent else '')

            filename = f'{config.plots_directory}instance-based-pruning-evaluation-queryset-{title}.pdf'
            grouped_bar_plot(
                observations,
                len(observation_labels),
                [labels_hash[approach] for approach in approaches],
                group_labels,
                observation_labels,
                filename=filename,
                hide_group_label=True,
#                 xlabel=title,
            )
        
    plt.show()
    
instance_pruning_plot(merged_evaluation,labels_hash,new_approaches)
```

```python
def query_matches_mrr_plot(evaluation,labels_hash,**kwargs):
    config=ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(5)]
    group_labels = ['group']

    observations = [
                [
                    evaluation[database][queryset]['standard']['query_matches'][metric]
                    for metric in metrics
                ]
                for database in evaluation
                for queryset in evaluation[database]   
            ]

    
    
    observation_labels = [metric.upper() for metric in metrics]

    color_labels = [labels_hash[queryset] 
                    for database in evaluation 
                    for queryset in evaluation[database]]

    filename = kwargs.get('filename',f'{config.plots_directory}qm_ranking.pdf')
    grouped_bar_plot(
        observations,
        len(observation_labels),
        color_labels,
        group_labels,
        observation_labels,
        filename=filename,
        hide_group_label=True,
        bbox_to_anchor=(1.11,1)
    )
    
    
query_matches_mrr_plot(evaluation,labels_hash)
```

#### CNs from golden QMs

```python
def instance_pruning_plot(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = ['mrr']+[f'p@{k+1}' for k in range(7)]
    group_labels = [name.upper() for name in metrics]

    data = [
        (
            database,
            [
                (
                    approach,
                    [
                        evaluation[database][queryset][approach]['candidate_networks'][metric]
                        for metric in metrics
                        for queryset in evaluation[database]   
                    ],
                )
                for approach in approaches
            ]
        )
        for database in evaluation
    ]
    
    for database,approach_results in data:
        observations = [results for approach,results in approach_results]       

        observation_labels = [
            labels_hash[queryset]
            for group in group_labels
            for queryset in evaluation[database]
        ]
        
        filename = f'{config.plots_directory}instance-based-pruning-evaluation-{database}.pdf'
        grouped_bar_plot(
            observations,
            len(observation_labels),
            [labels_hash[approach] for approach in approaches],
            group_labels,
            observation_labels,
            filename=filename,
        )
    plt.show()

instance_pruning_plot(assume_golden_qms_evaluation,labels_hash,golden_qms_approaches)
```

### Lineplot CNs per QM

```python
def plot_cns_per_qm(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = [f'p@{k+1}' for k in range(10)]
    dashes_order = [(1,0),(1,0),(2,2),(2,2)]

    data = [
        (
            labels_hash[dataset],
            [
                (
                    labels_hash[queryset],labels_hash[approach],
                    [
                        evaluation[dataset][queryset][approach]['candidate_networks'][metric]
                        for metric in metrics
                    ]
                )
                for queryset in evaluation[dataset]
                for approach in approaches
            ]
        )
        for dataset in evaluation
    ]


    
    x_data = range(1,10+1)
    for dataset,results in data:
        for (queryset,approach,y_data),dashes in zip(results,dashes_order):
            plt.plot(
                    x_data,
                    y_data,
                    linewidth = 3,
                    label = f'{approach}/{queryset[5:]}',
                    dashes = dashes
                )

        plt.xticks(np.arange(1, 10+1, 1.0))
        plt.xlabel('Rank Position K')
        plt.ylabel('P@K')
        plt.legend()
        plt.savefig(f'{config.plots_directory}{dataset}-cns-per-qms-precision-at-k.pdf')  
        plt.show()
    

plot_cns_per_qm(assume_golden_qms_evaluation,labels_hash,golden_qms_approaches)
```

```python
def plot_cns_per_qm(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = [f'p@{k+1}' for k in range(10)]
    dashes_order = [(1,0),(2,2),(1,0),(2,2)]

    data = [
        (
            labels_hash[approach],
            [
                (
                    labels_hash[queryset],labels_hash[dataset],
                    [
                        evaluation[dataset][queryset][approach]['candidate_networks'][metric]
                        for metric in metrics
                    ]
                )
                for dataset in evaluation
                for queryset in evaluation[dataset]
            ]
        )
        for approach in approaches
        
    ]

    x_data = range(1,10+1)
    for dataset,results in data:
        for (queryset,approach,y_data),dashes in zip(results,dashes_order):
            plt.plot(
                    x_data,
                    y_data,
                    linewidth = 3,
                    label = queryset,
                    dashes = dashes
                )

        plt.xticks(np.arange(1, 10+1, 1.0))
        plt.xlabel('Rank Position K')
        plt.ylabel('P@K')
        plt.legend()
        plt.savefig(f'{config.plots_directory}{dataset}-cns-per-qms-precision-at-k.pdf')  
        plt.show()
    

plot_cns_per_qm(assume_golden_qms_evaluation,labels_hash,golden_qms_approaches)
```

```python
data =  assume_golden_qms_evaluation
for dataset in data:
    for queryset in data[dataset]:
        for approach in data[dataset][queryset]:
            print(f'{queryset}-{approach}')
            print(data[dataset][queryset][approach]['candidate_networks'])
            not_found = [i+1 for i,position in enumerate(data[dataset][queryset][approach]['candidate_networks']['relevant_positions']) if position==-1]
            print(f'not found: {not_found}\n\n')

```

### QUEST Comparison CN Ranking


#### New values

```python
recall = np.array([1 if i<35 else 0 for i in range(50)])
recall_std_err = recall.std()/(len(recall)**0.5)  
recall_mean = recall.mean()
recall_mean,recall_std_err

old_size = 35
new_size = 50
old_precision_mean = 0.808
new_precision_mean = (old_precision_mean*old_size)/new_size
new_precision_mean

old_precision_std_err = 0.073
old_precision_std = old_precision_std_err*(old_size**0.5)
old_variance = old_precision_std**2
old_sum_square_deviation = old_variance*old_size
old_sum_square_deviation

# a(1-old_precision_mean)**2 + b(0-old_precision_mean)**2 = old_sum_square_deviation
# a+b = 35
# a = 35-b
# (35-b)*(1-old_precision_mean)**2 + b(-old_precision_mean)**2 = old_sum_square_deviation
# 35*(1-old_precision_mean)**2 -b(1-old_precision_mean)**2 + b(-old_precision_mean)**2 = old_sum_square_deviation
# -b(1-old_precision_mean)**2 + b(-old_precision_mean)**2 = old_sum_square_deviation-35*(1-old_precision_mean)**2
# b(-1*(1-old_precision_mean)**2 + (-old_precision_mean)**2)  = old_sum_square_deviation-35*(1-old_precision_mean)**2
# b = (old_sum_square_deviation-35*(1-old_precision_mean)**2) / (-1*(1-old_precision_mean)**2 + (-old_precision_mean)**2)

b = (old_sum_square_deviation-35*(1-old_precision_mean)**2) / (-1*(1-old_precision_mean)**2 + (-old_precision_mean)**2)
a = 35-b
a,b

new_sum_square_deviation = a*(1-new_precision_mean)**2 + b*(0-new_precision_mean)**2 + (new_size-old_size)*(0-new_precision_mean)**2
new_variance = new_sum_square_deviation/new_size
new_precision_std = new_variance**0.5
new_precision_std_err = new_precision_std/(new_size**0.5)
new_precision_mean,new_precision_std_err
```

```python
#data from QUEST paper

precision_hard_coded_from_quest = {
    'QUEST': (0.808, 0.073),
    'BANKS': (0.244, 0.044),
    'DISCOVER': (0.601, 0.043),
    'DISCOVER-II': (0.443, 0.046),
    'BANKS-II': (0.666, 0.042),
    'DPBF': (0.769, 0.037),
    'BLINKS': (0.83, 0.033),
    'STAR': (0.688, 0.042),
}



recall_hard_coded_from_quest = {
    'QUEST':(1,0),
    'BANKS':(0.332,0.043),
    'DISCOVER':(0.774,0.033),
    'DISCOVER-II':(0.788,0.033),
    'BANKS-II':(0.769,0.035),
    'DPBF':(0.955,0.016),
    'BLINKS':(0.968,0.011),
    'STAR':(0.616,0.044),
}



#data from Coffman paper

precision_hard_coded_from_coffman = {
#     'QUEST':( 0.565 , 0.071),
    'BANKS':( 0.32 , 0.038 ),
    'DISCOVER':( 0.638 , 0.04 ),
    'DISCOVER-II':( 0.499 , 0.041 ),
    'BANKS-II':( 0.699 , 0.038 ),
    'DPBF':( 0.792 , 0.033 ),
    'BLINKS':( 0.846 , 0.029 ),
    'STAR':( 0.719 , 0.038 ),
}


recall_hard_coded_from_coffman = {
#     'QUEST':  ( 0.7 , 0.065),
    'BANKS':  ( 0.4 , 0.04 ),
    'DISCOVER':  ( 0.799 , 0.03 ),
    'DISCOVER-II':  ( 0.811 , 0.03 ),
    'BANKS-II':  ( 0.794 , 0.033 ),
    'DPBF':  ( 0.962 , 0.015 ),
    'BLINKS ':  ( 0.975 , 0.01 ),
    'STAR':  ( 0.657 , 0.037 ),
}
```

```python
def plot_quest_comparison(evaluation,
                          labels_hash,
                          approaches,
                          other_approaches = False,
                          orientation='horizontal',
                          precision_hard_coded=precision_hard_coded_from_quest,
                          recall_hard_coded=recall_hard_coded_from_quest,
                          queries_condition=lambda num_query: True
                          
                         ):
    config = ConfigHandler()


    data = {
        'Recall':{},
        'P@1':{},
    }
    
#     queries_condition = lambda num_query: (20>=num_query and num_query<=24) or (35>=num_query and num_query<=44)

    for x,approach in enumerate(approaches):
        label = labels_hash[('QUEST',approach)]
        
        precision_at_1 = np.array(
            [       
                1 
                if position==1 else 0 
                for num_query,position in enumerate(evaluation['mondial_coffman']['coffman_mondial'][approach]['candidate_networks']['relevant_positions'])
                if queries_condition(num_query)
            ]
        )

        recall = np.array(
            [       
                1 
                if position!=-1 else 0 
                for num_query,position in enumerate(evaluation['mondial_coffman']['coffman_mondial'][approach]['candidate_networks']['relevant_positions'])
                if queries_condition(num_query)
            ]
        )

        precision_std_err = precision_at_1.std()/(len(precision_at_1)**0.5)    
        data['P@1'][label] = ( 
            precision_at_1.mean(),
            precision_std_err
        )

        recall_std_err = recall.std()/(len(recall)**0.5)
        data['Recall'][label] = (
            recall.mean(),
            recall_std_err
        )
    
    if other_approaches == False:
        data['P@1']['QUEST']    = precision_hard_coded['QUEST']
        data['Recall']['QUEST'] = recall_hard_coded['QUEST']
    else:
        data['P@1'].update(precision_hard_coded)
        data['Recall'].update(recall_hard_coded)    
        
    print(f'data: {data}\
    \nprecision_hard_coded:{precision_hard_coded}')

    for experiment in data:       
        if orientation=='horizontal':
            fig, ax = plt.subplots(figsize = [6,len(data['P@1'])*1.1])
            ax.set_xlabel(experiment, fontsize=14)
            ax.set_yticks(range(len(data[experiment])))
            ax.set_yticklabels(data[experiment], fontsize=12)
            ax.xaxis.grid(True)
        else:
            fig, ax = plt.subplots(figsize = [len(data['P@1'])*1.1,5])
            ax.set_ylabel(experiment, fontsize=14)
            ax.set_xticks(range(len(data[experiment])))
            ax.set_xticklabels(data[experiment], fontsize=12)
            ax.yaxis.grid(True)

        for x,(label,(mean,std_err)) in enumerate(data[experiment].items()):
            
            if orientation=='horizontal':
                ax.barh(x,
                       mean,
                       height=0.6,
                       xerr=std_err,
                       ecolor='black',
                       capsize=10,
                       fill=True
                  )
            else:
                ax.bar(x,
                       mean,
                       width=0.6,
                       yerr=std_err,
                       ecolor='black',
                       capsize=10,
                       fill=True
                  )
            

        plt.tight_layout()
        experiment_name = 'precision' if experiment == 'P@1' else 'recall'
        filename = f'{config.plots_directory}comparison-with-QUEST-{experiment_name}-50queries.pdf'
        plt.savefig(filename)
        plt.show()
    return data

plot_quest_comparison(evaluation,labels_hash,quest_approaches,
                      other_approaches = False, orientation ='horizontal',
                      precision_hard_coded=precision_hard_coded_from_quest,
                      recall_hard_coded=recall_hard_coded_from_quest,
                      queries_condition = lambda num_query:not((num_query>=20 and num_query<=24) or (num_query>=35 and num_query<=44))
                     )
plot_quest_comparison(evaluation,labels_hash,quest_approaches,
                      other_approaches = True, orientation ='vertical',
                      precision_hard_coded=precision_hard_coded_from_coffman,
                      recall_hard_coded=recall_hard_coded_from_coffman,
                     )
```

## Performance

```python
from statistics import fmean
def plot_performance_bar(evaluation,labels_hash,phases,approach='standard',vertical=True, symlog=False,figsize=[4.4, 4.8]):    
    config = ConfigHandler()
    
    querysets = [queryset for database in evaluation for queryset in evaluation[database]]
    
    data = [[],[],[],[]]
    tick_labels = []
    for database in evaluation:
        for queryset in evaluation[database]:
            tick_labels.append(labels_hash[queryset])
            for i,phase in enumerate(phases):
                data[i].append(fmean(evaluation[database][queryset][approach]['performance'][phase]))
     
    N = len(querysets)
    sequence = np.arange(N)
    previous = np.zeros(N)
    
    width = 0.7
    
    pp(data)
    
    print(f'data={data}')
    print(f'tick_labels={tick_labels}')
    print(f'querysets={querysets}')
    querysets
    
    if vertical:
        layout = 'vertical'
        fig, ax = plt.subplots(figsize=figsize)

        for i,phase in enumerate(phases):   
            plt.bar(sequence, data[i], width, label=labels_hash[phase],bottom=previous)
            previous+=data[i]
        plt.ylabel('Execution Time (s)')
        plt.xticks(sequence, tick_labels)
        if symlog:
            plt.yscale('symlog')
    else:
        layout = 'horizontal'
        fig, ax = plt.subplots(figsize=figsize)
        for i,phase in enumerate(phases):
            plt.barh(sequence, data[i], width, label=labels_hash[phase],left=previous)
            previous+=data[i]
        plt.xlabel('Execution Time (s)')
        plt.yticks(sequence, tick_labels)
        ax.invert_yaxis()
        if symlog:
            plt.xscale('symlog')
    

    plt.legend()
    
    filename = f'{config.plots_directory}performance-evaluation-{layout}.pdf'
    plt.savefig(filename)
    plt.show()

plot_performance_bar(evaluation,labels_hash,phases,vertical=True, symlog=True)
plot_performance_bar(evaluation,labels_hash,phases,vertical=False, symlog=True)
```

### Performance 1CN-IP

```python
phases = ['km', 'qm', 'cn']
```

```python
plot_performance_bar(evaluation,labels_hash,phases,approach='standard',vertical=False,figsize=[9,4.4])
```

### Performance per Database Checks

```python
topk_cns_per_qm_list = [1]
max_database_accesses_list = [1,2,3,4,5,6,7,1000]
approaches = ['ipruning']

merged_evaluation = {}
for max_database_accesses in max_database_accesses_list:
    for topk_cns_per_qm in topk_cns_per_qm_list:
        subfolder = f'ipruning_checks/{topk_cns_per_qm}cns_per_qm/{max_database_accesses}db_accesses/'
        for approach in approaches:
            evaluation = load_evaluation(approaches,subfolder=subfolder)
            for dataset in evaluation:
                merged_evaluation.setdefault(dataset,{})
                for queryset in evaluation[dataset]:
                    new_approach = f'{topk_cns_per_qm}{approach}'
                    db_checks = f'{max_database_accesses}dbckecks'
                    
                    merged_evaluation[dataset].setdefault(queryset,{})
                    merged_evaluation[dataset][queryset].setdefault(new_approach,{})
                    merged_evaluation[dataset][queryset][new_approach].setdefault(db_checks,{})

                    merged_evaluation[dataset][queryset][new_approach][db_checks] = evaluation[dataset][queryset][approach]

data = deepcopy(merged_evaluation)
for dataset in data:
    for queryset in data[dataset]:
        for approach in data[dataset][queryset]:
            time_list = [
                avg(data[dataset][queryset][approach][num_checks]['performance']['cn'])
                for num_checks in data[dataset][queryset][approach]
            ]
            data[dataset][queryset][approach] = time_list            
data
```

### Performance pruninng

```python
def performance_pruning_plot(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = ['time']
    group_labels = [name.upper() for name in metrics]

    data = [
        (
            database,
            [
                (
                    approach,
                    [
                        avg(evaluation[database][queryset][approach]['performance']['cn'])
                        for metric in metrics
                        for queryset in evaluation[database]   
                    ],
                )
                for approach in approaches
            ]
        )
        for database in evaluation
    ]
    
    for database,approach_results in data:
        
        if database == 'imdb_coffman_subset':
            show_legend=False
        else:
            show_legend=True
            
        
        
        observations = [results for approach,results in approach_results]       

        observation_labels = [
            labels_hash[queryset]
            for group in group_labels
            for queryset in evaluation[database]
        ]
        
        print(f'observation_labels={observation_labels}')
        
#         pp(observations)
#         print('-------------')
#         pp(observation_labels)
#         print('###########')
        
        filename = f'{config.plots_directory}performance-pruning-{database}.pdf'
        grouped_bar_plot(
            observations,
            len(observation_labels),
            [labels_hash[approach] for approach in approaches],
            group_labels,
            observation_labels,
            filename=filename,
            hide_group_label=True,
            ymin=None,
            ymax=None,       
            ylabel='Elapsed Time(s)',
            figsize = [5, 4.8],
            bbox_to_anchor= (1, 1),
            show_legend=show_legend,
        )
        
        print(database)
    plt.show()
        
#         print(observations)
# performance_pruning_plot(evaluation,labels_hash,approaches)
performance_pruning_plot(merged_evaluation,labels_hash,new_approaches)
```

```python
observation_labels={'imdb_coffman_subset':['IMDb', 'IMDb-DI'],'mondial_coffman':['MONDIAL', 'MONDIAL-DI']}
```

```python
new_approaches
```

### Performance pruning boxplot

```python
def grouped_boxplot(observations,n,color_labels,group_labels,observation_labels,**kwargs):
    m = len(color_labels)
    num_groups = len(group_labels)
    group_size = n//num_groups if num_groups > 0 else 0    
    
    hide_group_label = kwargs.get('hide_group_label',False)
    title = kwargs.get('title','')
    ylabel = kwargs.get('ylabel','')
    filename = kwargs.get('filename',None)
    observation_margin = kwargs.get('observation_margin',0.05) 
    subgroup_margin = kwargs.get('subgroup_margin',0.25)
    group_margin = kwargs.get('group_margin',0.8)
    bbox_to_anchor=kwargs.get('bbox_to_anchor', (1.1, 1.00))
    ymin=kwargs.get('ymin',0)
    ymax=kwargs.get('ymax',1)
    figsize = kwargs.get('figsize',[15, 4.8])
    show_legend = kwargs.get('show_legend',True)
    
    
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
            
            if not hide_group_label:
                group_x.append(middle_of_group)           
            last_group=last_observation+step
        else:
            step=1+subgroup_margin

    x=np.array(x)
    group_x = np.array(group_x)   

    fig, ax = plt.subplots(figsize = figsize)
    colors = [u'#1f77b4', u'#ff7f0e', u'#2ca02c', u'#d62728', u'#9467bd', u'#8c564b', u'#e377c2', u'#7f7f7f', u'#bcbd22', u'#17becf']
    for i in range(m):
        bplot = ax.boxplot( 
            data[i],
            positions = x+( (1-m)/2 + i)*width,
            widths = width*(1-2*observation_margin),
#             label=color_labels[i],
            vert=True,
            patch_artist=True
        )
        for patch in bplot['boxes']:
            patch.set_facecolor(colors[i])
        
    ax.set_xticks(x)
    ax.set_xticklabels(observation_labels)

    if not hide_group_label:
        ax.set_xticks(np.concatenate((x,group_x)))
        ax.set_xticklabels(observation_labels+group_labels)

    ax.set_ylim(ymin=ymin, ymax=ymax)
    ax.set_ylabel(ylabel)
    
    if show_legend:
        legend=ax.legend(bbox_to_anchor=bbox_to_anchor)
        bbox_extra_artists = (legend,)
    else:
        bbox_extra_artists = None
    
    if filename is not None:
        plt.savefig(filename, bbox_extra_artists=bbox_extra_artists, bbox_inches='tight')
    
    if show_legend:
        colors=[]
        fig = plt.figure(figsize=(2, 1.25))
        patches = [
            mpatches.Patch(color=color, label=label)
            for label, color in zip(color_labels, colors)]
        fig.legend(patches, color_labels, loc='center', frameon=False)
        plt.show()

    return plt
```

```python
def performance_pruning_boxplot(evaluation,labels_hash,approaches):
    config = ConfigHandler()
    metrics = ['time']
    group_labels = [name.upper() for name in metrics]

    data = [
        (
            database,
            [
                (
                    approach,
                    [
                        evaluation[database][queryset][approach]['performance']['cn']
                        for metric in metrics
                        for queryset in evaluation[database]   
                    ],
                )
                for approach in approaches
            ]
        )
        for database in evaluation
    ]
    
    for database,approach_results in data:
        
        if database == 'imdb_coffman_subset':
            show_legend=False
        else:
            show_legend=True
            
        
        
        observations = [results for approach,results in approach_results]       

        observation_labels = [
            labels_hash[queryset]
            for group in group_labels
            for queryset in evaluation[database]
        ]
        
#         pp(observations)
#         print('-------------')
#         pp(observation_labels)
#         print('###########')
        
        filename = f'{config.plots_directory}performance-pruning-boxplot-{database}.pdf'
        grouped_boxplot(
            observations,
            len(observation_labels),
            [labels_hash[approach] for approach in approaches],
            group_labels,
            observation_labels,
            filename=filename,
            hide_group_label=True,
            ymin=None,
            ymax=None,       
            ylabel='Elapsed Time(s)',
            figsize = [9, 4.8],
            bbox_to_anchor= (1, 1),
            show_legend=show_legend,
            observation_margin = 0.05,
            subgroup_margin = 0.15,
            group_margin =0.8,
        )
        
        print(database)
    plt.show()
        
#         print(observations)
# performance_pruning_plot(evaluation,labels_hash,approaches)
performance_pruning_boxplot(merged_evaluation,labels_hash,new_approaches)
```

```python
merged_evaluation['mondial_coffman']['coffman_mondial']['9standard']
```

```python
evaluation['mondial_coffman']['coffman_mondial']['standard'].keys()
```

```python
for i in range(50):
    data = merged_evaluation['mondial_coffman']['coffman_mondial']['1ipruning']
    cn_time = data['performance']['cn'][i]
    num_kms = data['num_keyword_matches'][i]
    num_qms = data['num_query_matches'][i]
    num_cns = data['num_candidate_networks'][i]
    
    print(f'{cn_time:.8f};{num_kms};{num_qms};{num_cns}')
```

## Weighting Schemes Analysis

```python
candidate_networkssubfolder = 'cn_generation/1cns_per_qm/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_performance_bar(evaluation,labels_hash,phases,vertical=False)
```

```python
# for weight_scheme in different_ws_evaluations:
#     for dataset in different_ws_evaluations[weight_scheme]:
#         for queryset in different_ws_evaluations[weight_scheme][dataset]:
#             for approach in approaches:
#                 print(f'{dataset} {queryset} {weight_scheme} {approach}')
                
                
```

```python
# metrics = ['mrr']+[f'p@{k+1}' for k in range(4)]
# group_labels = [name.upper() for name in metrics]

# weight_schemes = [f'ws{i}' for i in range(4)]

# results = {}
# lines = []

# subfolder = 'old_cn_per_qm/1cns_per_qm/'

# for weight_scheme in weight_schemes:
#     evaluation = load_evaluations(approaches,subfolder=f'{subfolder}{weight_scheme}/')
#     values = [
#         (dataset[
#             '{:.2f}'.format(evaluation[dataset][queryset][approach]['candidate_networks'][metric])
#             for metric in metrics
#             for queryset in evaluation[dataset]
#             for approach in approaches
#         ])
#         for dataset in evaluation
#     ]
#     cur_line = ', '.join([str(weight_scheme)]+values)
#     lines.append(cur_line)
        
# for line in lines:
#     print(line)
```

```python
# metrics = ['mrr']+[f'p@{k+1}' for k in range(5)]
# group_labels = [name.upper() for name in metrics]

# weighting_schemes = list(range(4))

# results = {}

# lines = []
# for database in databases: 
#     lines.append(database)
#     title_querysets = [labels[queryset] for queryset in labels_hash[database]]    
#     title_line1= ', '.join(title_querysets)
#     lines.append(title_line1)
    
#     title_approaches = list(approaches.values())*len(querysets_hash[database])*len(metrics)
#     title_line2 = ', '.join(['scheme']+title_approaches)
#     lines.append(title_line2)
#     for weighting_scheme in weighting_schemes:
#         subfolder = f'weighting_scheme_{weighting_scheme}/'
#         evaluation = load_evaluations(databases,approaches,subfolder=subfolder)
        
#         values = [
#             '{:.2f}'.format(evaluation[database][queryset][approach]['candidate_networks'][metric])
#             for metric in metrics
#             for queryset in querysets_hash[database]
#             for approach in approaches
#         ]
#         cur_line = ', '.join([str(weighting_scheme)]+values)
#         lines.append(cur_line)
        
# for line in lines:
#     print(line)
```
