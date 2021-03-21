import sys
from json import load

sys.path.append('../')

from utils import ConfigHandler, last_path
from mapper import Mapper
from evaluation import EvaluationHandler


import matplotlib
import matplotlib.pyplot as plt
import numpy as np


approaches = ['standard']
labels = ['MOND','MOND-DI','IMDb','IMDb-DI']
querysets = ['mondial_coffman','mondial_coffman_clear','imdb','imdb_clear']


km_data = []
qm_data = []
for j,queryset in enumerate(querysets):
    for i,approach in enumerate(approaches):
        config = ConfigHandler(reset = True,database=queryset)
        results_filename = last_path(f'{config.results_directory}{config.database_config}-{approach}-%03d.json')
        with open(results_filename,mode='r') as f:
            data = load(f)['evaluation']
            
        km_data.append(data['num_keyword_matches'])
        qm_data.append(data['num_query_matches'])


filename = f'{config.plots_directory}num_keyword_matches.pdf'
plt.boxplot(km_data,vert=0,patch_artist=True,labels=labels)
plt.xscale('log')
plt.xlabel('Num. Keyword Matches')
if filename is not None:
    plt.savefig(filename)


filename = f'{config.plots_directory}num_query_matches.pdf'
plt.boxplot(qm_data,vert=0,patch_artist=True,labels=labels)
plt.xscale('log')
plt.xlabel('Num. Query Matches')
if filename is not None:
    plt.savefig(filename)