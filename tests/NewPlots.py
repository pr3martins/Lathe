# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.9.1
#   kernelspec:
#     display_name: k2db
#     language: python
#     name: k2db
# ---

# ## Load Evaluation Results

# +
from json import load
from pprint import pprint as pp
import matplotlib.pyplot as plt
import numpy as np

from k2db.utils import ConfigHandler, Similarity, last_path
from k2db.mapper import Mapper
from k2db.evaluation import EvaluationHandler


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
# subfolder = '10_repeat_cn_generation/1cns_per_qm/'
subfolder = 'ipruning_checks/1cns_per_qm/7db_accesses/'


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
    'coffman_mondial':'MOND',
    'coffman_mondial_clear_intents':'MOND-DI',
    #approaches
    'standard':'CN-Std',
    'pospruning':'CN-Pos',
    'prepruning':'CN-Pre',
    'ipruning':'CN-IP',
    #comparison with quest approaches
    ('QUEST','ipruning'):'Lathe',
    #phases
    'skm':'Schema-Keyword Match',
    'vkm':'Value-Keyword Match',
    'qm':'Query Match',
    'cn':'Candidate Network',
}

phases = ['skm','vkm','qm','cn']

evaluation = load_evaluation(approaches,subfolder=subfolder)

assume_golden_qms_evaluation = load_evaluation(golden_qms_approaches,subfolder=golden_qms_subfolder)

# -

# ## Num. Keyword Matches

# +
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


# -

# ## Num. Query Matches

# +
def plot_num_query_matches(evaluation,labels_hash): 
    config = ConfigHandler()    
    data = []
    labels = []
    
    margin = 0.05
#     xlim = [-2*margin, 10**(3+margin)]
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


# -

# ## Query Matches Max relevant position

# +
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


# -

# ## Query Match Ranking

# +
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


# +
def query_matches_mrr_plot(evaluation,labels_hash):
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
    plt.ylabel('MRR')
    plt.legend()
    plt.savefig(f'{config.plots_directory}qms-precision-at-k.pdf')  
    plt.show()

query_matches_mrr_plot(evaluation,labels_hash)


# -

# ## Candidate Network Ranking

# +
# from json import load
# import matplotlib
# import matplotlib.pyplot as plt
# import numpy as np

# from k2db.utils import ConfigHandler, last_path
# from k2db.mapper import Mapper
# from k2db.evaluation import EvaluationHandler

# config = ConfigHandler()
# -

# ### Grouped Barplot

def grouped_bar_plot(observations,n,color_labels,group_labels,observation_labels,**kwargs):
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

    fig, ax = plt.subplots(figsize = [15, 4.8])
    for i in range(m):
        ax.bar(x+( (1-m)/2 + i)*width, data[i], width*(1-2*observation_margin), label=color_labels[i])

    ax.set_xticks(x)
    ax.set_xticklabels(observation_labels)

    if not hide_group_label:
        ax.set_xticks(np.concatenate((x,group_x)))
        ax.set_xticklabels(observation_labels+group_labels)

    ax.set_ylim(ymin=0, ymax=1)

    lgd=ax.legend(bbox_to_anchor=bbox_to_anchor)

    if filename is not None:
        plt.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')
        
    return plt


# ### Standard CN Ranking

# +
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


# -

# ### Instance Pruning CN Ranking

# +
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
        
#         print(observations)
instance_pruning_plot(evaluation,labels_hash,approaches)
# -

merged_evaluation = {}
for i in [1,2,4,7]:
    subfolder = f'cn_generation/{i}cns_per_qm/'
    print(f'{i} CNs per QM')
    evaluation = load_evaluation(approaches,subfolder=subfolder)
    for dataset in evaluation:
        merged_evaluation.setdefault(dataset,{})
        
        for queryset in evaluation[dataset]:
            merged_evaluation[dataset].setdefault(queryset,{})
            
            for approach in evaluation[dataset][queryset]:
                new_approach = f'{i}{approach}'
                merged_evaluation[dataset][queryset][new_approach] = evaluation[dataset][queryset][approach]
                
    instance_pruning_plot(evaluation,labels_hash,approaches)

new_approaches = ['1standard', '4standard', '7standard','1ipruning','2ipruning']
for approach in new_approaches:
    i = approach[0]
    root_label = labels_hash[approach[1:]][3:]
    labels_hash[approach] = f'{i}CN-{root_label}'
instance_pruning_plot(merged_evaluation,labels_hash,new_approaches)


# #### CNs from golden QMs

# +
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


# -

# ### Lineplot CNs per QM

# +
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


# +
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
        plt.savefig(f'{config.plots_directory}{approach}-cns-per-qms-precision-at-k.pdf')  
        plt.show()
    

plot_cns_per_qm(assume_golden_qms_evaluation,labels_hash,golden_qms_approaches)


# -

# ### QUEST Comparison CN Ranking

# +
def plot_quest_comparison(evaluation,labels_hash,approaches,other_approaches = False):
    config = ConfigHandler()
    precision_hard_coded = {
        'QUEST': (0.808, 0.073),
#         'BANKS': (0.244, 0.044),
#         'DISCOVER': (0.601, 0.043),
#         'DISCOVER-II': (0.443, 0.046),
#         'BANKS-II': (0.666, 0.042),
#         'DPBF': (0.769, 0.037),
#         'BLINKS': (0.83, 0.033),
#         'STAR': (0.688, 0.042),
        'Lathe45': (0.956, 0.0307),
    }

    {'Recall': {'Lathe': (0.9777777777777777, 0.02197392144324641)},
 'P@1': {'Lathe': (0.9555555555555556, 0.030720653856781806)}}
    

    recall_hard_coded = {
        'QUEST':(1,0),
#         'BANKS':(0.332,0.043),
#         'DISCOVER':(0.774,0.033),
#         'DISCOVER-II':(0.788,0.033),
#         'BANKS-II':(0.769,0.035),
#         'DPBF':(0.955,0.016),
#         'BLINKS':(0.968,0.011),
#         'STAR':(0.616,0.044),
        'Lathe45': (0.977, 0.0219),
        }


    data = {
        'Recall':{},
        'P@1':{},
    }
    
    queries_condition = lambda num_query: num_query<30 or num_query>39

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

    for experiment in data:
#         fig, ax = plt.subplots(figsize = [len(data['P@1'])*1.1, 4.8])
        fig, ax = plt.subplots(figsize = [6,len(data['P@1'])*1.1])

        for y,(label,(mean,std_err)) in enumerate(data[experiment].items()):
            ax.barh(y,
                   mean,
                   height=0.6,
                   xerr=std_err,
                   ecolor='black',
                   capsize=10,
                   fill=True
              )


        ax.set_xlabel(experiment, fontsize=14)
        ax.set_yticks(range(len(data[experiment])))
        ax.set_yticklabels(data[experiment], fontsize=12)

#         ax.set_title('Comparison with other approaches')
        ax.xaxis.grid(True)

        plt.tight_layout()
        experiment_name = 'precision' if experiment == 'P@1' else 'recall'
        filename = f'{config.plots_directory}comparison-with-QUEST-{experiment_name}-35queries.pdf'
        plt.savefig(filename)
        plt.show()
    return data

plot_quest_comparison(evaluation,labels_hash,quest_approaches,other_approaches = False)
# -



# #### New values

recall = np.array([1 if i<35 else 0 for i in range(45)])
recall_std_err = recall.std()/(len(recall)**0.5)  
recall_mean = recall.mean()
recall_mean,recall_std_err

# +
old_size = 35
new_size = 45
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


# +
def plot_quest_comparison(evaluation,labels_hash,approaches,other_approaches = False):
    config = ConfigHandler()
    precision_hard_coded = {
        'QUEST': (0.6284444444444445, 0.07358743091572538),
    }


    recall_hard_coded = {
        'QUEST':(0.7777777777777778, 0.06197481678030189),
        }


    data = {
        'Recall':{},
        'P@1':{},
    }
    
    queries_condition = lambda num_query: True

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

    for experiment in data:
        fig, ax = plt.subplots(figsize = [len(data['P@1'])*1.1, 4.8])

        for x,(label,(mean,std_err)) in enumerate(data[experiment].items()):
            ax.bar(x,
                   mean,
                   width=0.6,
                   yerr=std_err,
                   ecolor='black',
                   capsize=10,
                   fill=True
              )


        ax.set_ylabel(experiment)

        ax.set_xticks(range(len(data[experiment])))
        ax.set_xticklabels(data[experiment])

#         ax.set_title('Comparison with other approaches')
        ax.yaxis.grid(True)

        plt.tight_layout()
        experiment_name = 'precision' if experiment == 'P@1' else 'recall'
        filename = f'{config.plots_directory}comparison-with-QUEST-{experiment_name}-45queries.pdf'
        plt.savefig(filename)
        plt.show()
        return data
        
plot_quest_comparison(evaluation,labels_hash,quest_approaches,other_approaches = False)


# -

# ## Compare only lath using 45 queries

# +
def plot_quest_comparison(evaluation,labels_hash,approaches,other_approaches = False):
    config = ConfigHandler()


    data = {
        'Recall':{},
        'P@1':{},
    }
    
    queries_condition = lambda num_query: 1

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

#         fig, ax = plt.subplots(figsize = [len(data['P@1'])*1.1, 4.8])
#     fig, ax = plt.subplots(figsize = [6,len(data['P@1'])*1.1])
    fig, ax = plt.subplots(figsize = [6,len(data['P@1'])*2])
    colors = ['red','green']
    for i,experiment in enumerate(data):
        for j,(label,(mean,std_err)) in enumerate(data[experiment].items()):
            y=i+1*j
            ax.barh(y,
                   mean,
                   height=0.6,
                   xerr=std_err,
                   ecolor='black',
                   capsize=10,
                   fill=True,
                    color=colors[i],
              )


#     ax.set_xlabel('Lathe with 45 queries', fontsize=14)
    ax.set_yticks([0,1])
    ax.set_yticklabels(['Recall','P@1'], fontsize=12)

#         ax.set_title('Comparison with other approaches')
    ax.xaxis.grid(True)

    plt.tight_layout()
    experiment_name = 'precision' if experiment == 'P@1' else 'recall'
    filename = f'{config.plots_directory}comparison-with-QUEST-{experiment_name}-45queries.pdf'
    plt.savefig(filename)
    plt.show()
    return data

plot_quest_comparison(evaluation,labels_hash,quest_approaches,other_approaches = False)
# -

# ## Performance

# +
from statistics import fmean
def plot_performance_bar(evaluation,labels_hash,phases,vertical=True):    
    config = ConfigHandler()
    
    querysets = [queryset for database in evaluation for queryset in evaluation[database]]
    
    data = [[],[],[],[]]
    tick_labels = []
    for database in evaluation:
        for queryset in evaluation[database]:
            tick_labels.append(labels_hash[queryset])
            for i,phase in enumerate(phases):
                data[i].append(fmean(evaluation[database][queryset]['standard']['performance'][phase]))
           
    N = len(querysets)
    sequence = np.arange(N)
    previous = np.zeros(N)
    
    width = 0.7
    
    if vertical:
        layout = 'vertical'
        figsize = [4.4, 4.8]
        fig, ax = plt.subplots(figsize=figsize)

        for i,phase in enumerate(phases):   
            plt.bar(sequence, data[i], width, label=labels_hash[phase],bottom=previous)
            previous+=data[i]
        plt.ylabel('Execution Time (s)')
        plt.xticks(sequence, tick_labels)
    else:
        layout = 'horizontal'
        fig, ax = plt.subplots()
        for i,phase in enumerate(phases):
            plt.barh(sequence, data[i], width, label=labels_hash[phase],left=previous)
            previous+=data[i]
        plt.xlabel('Execution Time (s)')
        plt.yticks(sequence, tick_labels)
        ax.invert_yaxis()

    plt.legend()
    
    filename = f'{config.plots_directory}performance-evaluation-{layout}.pdf'
    plt.savefig(filename)
    plt.show()

plot_performance_bar(evaluation,labels_hash,phases,vertical=True)
plot_performance_bar(evaluation,labels_hash,phases,vertical=False)
# -

# ### Performance per Database Checks

# +
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
# -

from copy import deepcopy
def avg(x):
    return sum(x)/len(x)


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

# ## Weighting Schemes Analysis

subfolder = 'cn_generation/1cns_per_qm/'
evaluation = load_evaluation(approaches,subfolder=subfolder)
plot_performance_bar(evaluation,labels_hash,phases,vertical=False)

# +
# for weight_scheme in different_ws_evaluations:
#     for dataset in different_ws_evaluations[weight_scheme]:
#         for queryset in different_ws_evaluations[weight_scheme][dataset]:
#             for approach in approaches:
#                 print(f'{dataset} {queryset} {weight_scheme} {approach}')
                
                

# +
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

# +
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
