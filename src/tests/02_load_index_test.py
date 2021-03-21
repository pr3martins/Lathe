import sys
import os
from pprint import pprint as pp
from glob import glob
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler, BabelHash

print(os.path.abspath(__file__))
datasets = ['imdb_renamed','imdb_coffman','mondial']
for dataset in datasets:
    print(f'Database {dataset}')
    dataset_config_file = f'../../config/dataset_configs/{dataset}.json'
    config = ConfigHandler(reset=True,dataset_config_file=dataset_config_file)

    indexHandler = IndexHandler(config=config)
    indexHandler.load_indexes(
        config.value_index_filename,
        config.schema_index_filename,
        load_method='sample',
        sample_size=500,
    )

    print('Schema Index:\n')
    pp(dict(indexHandler.schema_index))

    print('\n\nValue Index (Sampling 15 elements):\n')
    print(list(indexHandler.value_index.keys()))
    print('#'*100)
