import sys
import os
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler

print(os.path.abspath(__file__))
datasets = ['mondial','imdb_renamed','imdb_coffman']
for dataset in datasets:
    print(f'Database {dataset}')
    dataset_config_file = f'../../config/dataset_configs/{dataset}.json'
    config = ConfigHandler(reset=True,dataset_config_file=dataset_config_file)
    config.set_logging_level('INFO')
    indexHandler = IndexHandler(config=config)
    indexHandler.create_indexes()
