from pprint import pprint as pp

from k2db.utils import ConfigHandler
from k2db.index import IndexHandler

datasets = ['imdb_renamed','imdb_coffman','mondial']
sample_size = 100

for dataset in datasets:
    print(f'Database {dataset}')
    dataset_config_filepath = f'dataset_configs/{dataset}.json'
    config = ConfigHandler(reset=True,dataset_config_filepath=dataset_config_filepath)

    indexHandler = IndexHandler(config=config)
    indexHandler.load_indexes(
        load_method='sample',
        sample_size=sample_size,
    )
    print(f'Schema Graph:\n{indexHandler.schema_graph}')
    print(f'Edge infos:')
    pp(indexHandler.schema_graph._edges_info)

    print('\n\nSchema Index:\n')
    pp(dict(indexHandler.schema_index))

    print(f'\n\nValue Index (Sampling {sample_size} elements):\n')
    print(list(indexHandler.value_index.keys()))


    print('#'*100)
