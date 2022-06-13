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

```python
from json import load
from pprint import pprint as pp
import matplotlib.pyplot as plt
import numpy as np
from copy import deepcopy
import re

plots_directory = '../config/../plots/'


labels_hash={'imdb_coffman_subset': 'IMDb',
 'mondial_coffman': 'Mondial',
 'coffman_imdb': 'IMDb',
 'coffman_imdb_clear_intents': 'IMDb-DI',
 'coffman_mondial': 'MONDIAL',
 'coffman_mondial_clear_intents': 'MONDIAL-DI',
 'standard': 'CN-Std',
 'pospruning': 'CN-Pos',
 'prepruning': 'CN-Pre',
 'ipruning': 'CN-IP',
 ('QUEST', 'ipruning'): 'LATHE',
 ('QUEST', 'standard'): 'LATHE',
 'km': 'Keyword Match',
 'skm': 'Schema-Keyword Match',
 'vkm': 'Value-Keyword Match',
 'qm': 'Query Match',
 'cn': 'Candidate Network'}
```

```python
def query_matches_precision_plot(plots_directory):
    metrics = [f'p@{k+1}' for k in range(10)]
    dashes_order = [(1,0),(2,2),(1,0),(2,2)]
    precision_data=[('IMDb', [0.58, 0.84, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('IMDb-DI', [0.58, 0.8, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('MONDIAL', [0.88, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]), ('MONDIAL-DI', [0.88, 0.98, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0])]
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
    plt.savefig(f'{plots_directory}qms-precision-at-k.pdf', bbox_inches='tight')  
    plt.show()

query_matches_precision_plot(plots_directory)
```

```python
def query_matches_mrr_plot():
    color_labels = ['IMDb', 'IMDb-DI', 'MONDIAL', 'MONDIAL-DI']
    data = [0.7523333333333333, 0.7456666666666668, 0.94, 0.9366666666666668]
    
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
    
    plt.xlabel('MRR')
    
    filename = f'{plots_directory}qm_ranking_mrr.pdf'
    
    plt.savefig(filename, bbox_inches='tight')
    plt.show()

    # ax.set_yticklabels(color_labels, fontsize=12)
query_matches_mrr_plot()
```

```python
def plot_quest_comparison(
                          labels_hash,
                          other_approaches = False,
                          orientation='horizontal',
                          queries_condition=lambda num_query: True):
    data= {'Recall': {'LATHE': (1.0, 0.0), 'QUEST': (1, 0)}, 'P@1': {'LATHE': (0.9714285714285714, 0.028160307445976064), 'QUEST': (0.808, 0.073)}} 

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
        filename = f'{plots_directory}comparison-with-QUEST-{experiment_name}-50queries.pdf'
        plt.savefig(filename, bbox_inches='tight')
        plt.show()
    return data

plot_quest_comparison(labels_hash,other_approaches = False,
                      orientation ='horizontal',
                      queries_condition = lambda num_query:not((num_query>=20 and num_query<=24) or (num_query>=35 and num_query<=44))
                     )                     
```

```python
def plot_quest_comparison(
                          labels_hash,
                          other_approaches = False,
                          orientation='horizontal',
                          queries_condition=lambda num_query: True):
    data= {'Recall': {'LATHE': (0.98, 0.019798989873223326), 'BANKS': (0.4, 0.04), 'DISCOVER': (0.799, 0.03), 'DISCOVER-II': (0.811, 0.03), 'BANKS-II': (0.794, 0.033), 'DPBF': (0.962, 0.015), 'BLINKS ': (0.975, 0.01), 'STAR': (0.657, 0.037)}, 'P@1': {'LATHE': (0.96, 0.027712812921102038), 'BANKS': (0.32, 0.038), 'DISCOVER': (0.638, 0.04), 'DISCOVER-II': (0.499, 0.041), 'BANKS-II': (0.699, 0.038), 'DPBF': (0.792, 0.033), 'BLINKS': (0.846, 0.029), 'STAR': (0.719, 0.038)}} 

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
        filename = f'{plots_directory}comparison-with-QUEST-{experiment_name}-50queries.pdf'
        plt.savefig(filename, bbox_inches='tight')
        plt.show()
    return data

plot_quest_comparison(labels_hash,other_approaches = True,
                      orientation ='vertical',
                     )                     
```

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

```python
# metrics = ['mrr']+[f'p@{k+1}' for k in range(10)]
# observation_labels = [metric.upper() for metric in metrics]
# observation_labels
```

```python
def instance_pruning_plot(labels_hash,approaches):
    group_labels = ['group']

    data=[(False, [('imdb_coffman_subset', [('1standard', [0.7290000000000001, 0.54, 0.82, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('2standard', [0.7133333333333332, 0.54, 0.76, 0.88, 0.96, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98]), ('8standard', [0.704857142857143, 0.54, 0.76, 0.82, 0.9, 0.96, 0.96, 0.98, 0.98, 0.98, 0.98]), ('9standard', [0.704857142857143, 0.54, 0.76, 0.82, 0.9, 0.96, 0.96, 0.98, 0.98, 0.98, 0.98]), ('1ipruning', [0.7823333333333333, 0.64, 0.84, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('2ipruning', [0.7733333333333331, 0.64, 0.8, 0.92, 0.96, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98])]), ('mondial_coffman', [('1standard', [0.75, 0.72, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78]), ('2standard', [0.7566666666666666, 0.72, 0.78, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8]), ('8standard', [0.7603571428571427, 0.72, 0.78, 0.78, 0.78, 0.78, 0.78, 0.8, 0.86, 0.86, 0.86]), ('9standard', [0.7746197691197694, 0.72, 0.78, 0.78, 0.78, 0.78, 0.78, 0.8, 0.86, 0.9, 0.98]), ('1ipruning', [0.97, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('2ipruning', [0.98, 0.96, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0])])]), (True, [('imdb_coffman_subset', [('1standard', [0.7290000000000001, 0.54, 0.82, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('2standard', [0.7173333333333332, 0.54, 0.78, 0.9, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98]), ('8standard', [0.7110238095238095, 0.54, 0.78, 0.86, 0.92, 0.94, 0.94, 0.96, 0.98, 0.98, 0.98]), ('9standard', [0.7110238095238095, 0.54, 0.78, 0.86, 0.92, 0.94, 0.94, 0.96, 0.98, 0.98, 0.98]), ('1ipruning', [0.7956666666666666, 0.66, 0.86, 0.94, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('2ipruning', [0.7833333333333331, 0.66, 0.8, 0.92, 0.96, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98])]), ('mondial_coffman', [('1standard', [0.75, 0.72, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78, 0.78]), ('2standard', [0.7566666666666666, 0.72, 0.78, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8]), ('8standard', [0.7770238095238095, 0.72, 0.78, 0.78, 0.78, 0.78, 0.78, 0.8, 0.98, 0.98, 0.98]), ('9standard', [0.7770238095238095, 0.72, 0.78, 0.78, 0.78, 0.78, 0.78, 0.8, 0.98, 0.98, 0.98]), ('1ipruning', [0.97, 0.96, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98, 0.98]), ('2ipruning', [0.98, 0.96, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0])])])]
    
    for clear_intent,database_results in data:
        for database,approach_results in database_results:
            observations = [results for approach,results in approach_results]       

            observation_labels = ['MRR', 'P@1', 'P@2', 'P@3', 'P@4', 'P@5', 'P@6', 'P@7', 'P@8', 'P@9', 'P@10']

#             pp(observations)
#             print('-------------')
#             pp(observation_labels)
#             print('###########')

            title = labels_hash[database] + ('-DI' if clear_intent else '')
            print(title)
            filename = f'{plots_directory}instance-based-pruning-evaluation-queryset-{title}.pdf'
            grouped_bar_plot(
                observations,
                len(observation_labels),
                ['5/1/0', '5/2/0', '5/8/0', '5/9/0', '5/1/9', '5/2/9'],
                group_labels,
                observation_labels,
                filename=filename,
                hide_group_label=True,
#                 xlabel=title,
            )
        
    plt.show()
    
instance_pruning_plot(labels_hash,approaches=['1standard', '2standard', '8standard', '9standard', '1ipruning', '2ipruning'])
```

```python
from statistics import fmean
def plot_performance_bar(labels_hash,phases,approach='standard',vertical=True, symlog=False,figsize=[4.4, 4.8]):    
    
    querysets=['coffman_imdb', 'coffman_imdb_clear_intents', 'coffman_mondial', 'coffman_mondial_clear_intents']    
    tick_labels=['IMDb', 'IMDb-DI', 'MONDIAL', 'MONDIAL-DI']
    data=[[0.12200531636015512, 0.1247905136402187, 0.11224583773997437, 0.13512292325995076], [0.13611626477984828, 0.16514443615978963, 0.0011655362000965396, 0.003187579700170318], [0.02115736674008076, 0.031700986440082485, 0.4970339208399309, 1.5286474679399544], []]
    
    N = len(querysets)
    sequence = np.arange(N)
    previous = np.zeros(N)
    
    width = 0.7
    
    pp(data)
    
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
    
    filename = f'{plots_directory}performance-evaluation-{layout}.pdf'
    plt.savefig(filename,bbox_inches = 'tight')
    plt.show()

phases = ['km', 'qm', 'cn']
plot_performance_bar(labels_hash,phases,approach='standard',vertical=False,figsize=[6,3])
```

```python
def performance_pruning_plot(labels_hash,approaches):

    metrics = ['time']
    group_labels = [name.upper() for name in metrics]

    data=[('imdb_coffman_subset', [('1standard', [0.02115736674008076, 0.031700986440082485]), ('2standard', [0.12058234440017258, 0.14090778076013522]), ('8standard', [0.15847932011980448, 0.1747323750797659]), ('9standard', [0.15889015827953698, 0.1751033083992661]), ('1ipruning', [0.1893679195200457, 0.1346787598600349]), ('2ipruning', [0.3273818460400071, 0.2543204163400514])]), ('mondial_coffman', [('1standard', [0.4970339208399309, 1.5286474679399544]), ('2standard', [2.3452199654799553, 2.9771256134799478]), ('8standard', [12.980933825479733, 11.327688257419796]), ('9standard', [13.087275288859963, 11.4389506635601]), ('1ipruning', [6.327919981700034, 5.25000847403986]), ('2ipruning', [10.827171132780059, 9.81748693902009])])]
    
    for database,approach_results in data:
        
        if database == 'imdb_coffman_subset':
            show_legend=False
        else:
            show_legend=True
            
        
        
        observations = [results for approach,results in approach_results]       

        observation_labels={'imdb_coffman_subset':['IMDb', 'IMDb-DI'],'mondial_coffman':['MONDIAL', 'MONDIAL-DI']}
        
#         pp(observations)
#         print('-------------')
#         pp(observation_labels)
#         print('###########')
        
        filename = f'{plots_directory}performance-pruning-{database}.pdf'
        grouped_bar_plot(
            observations,
            len(observation_labels),
            ['5/1/0', '5/2/0', '5/8/0', '5/9/0', '5/1/9', '5/2/9'],
            group_labels,
            observation_labels[database],
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
performance_pruning_plot(labels_hash,approaches=['1standard', '2standard', '8standard', '9standard', '1ipruning', '2ipruning'])
```
