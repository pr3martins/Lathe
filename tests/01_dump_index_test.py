from k2db.utils import ConfigHandler
from k2db.index import IndexHandler

datasets = ['mondial','imdb_renamed','imdb_coffman']
for dataset in datasets:
    print(f'Database {dataset}')
    dataset_config_filepath = f'dataset_configs/{dataset}.json'
    config = ConfigHandler(reset=True,dataset_config_filepath=dataset_config_filepath)
    config.set_logging_level('INFO')
    indexHandler = IndexHandler(config=config)
    indexHandler.create_indexes()
