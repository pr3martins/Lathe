import json
import os
import logging

debug_mapping = { \
	'DEBUG' : logging.DEBUG,\
	'INFO': logging.INFO, \
	'ERROR': logging.ERROR, \
	'WARNING': logging.WARNING, \
	'CRITICAL': logging.CRITICAL \
}


'''
    Class responsible to hold and managing the configuration:

    General Configuration Attributes:
        - connection
            - host
            - password
            - user
        - logging_level
        - database_config
        - parameter_values
             - <class>.<function> : { arg1 : value }

    Specific Configuration Attributes:
        - remove_from_index : [table.column]
        - database_name
        - schema_index_file
        - value_index_file
        - edges_file
        - evaluation
            - query_gt_file
            - parameter_values
'''
class ConfigHandler:
    __instance = None
    def __init__(self, database='', reset=False):

        if ConfigHandler.__instance is None or reset:
            #reading general config
            config_partial_path = os.path.abspath(__file__).split('/')[:-3] + ['config']
            config_path = '/'.join(config_partial_path + ['config.json'])
            config_file = open(config_path, 'r')
            ConfigHandler.__instance = json.load(config_file)
            ConfigHandler.__instance.setdefault('table_file', None)

            #getting the specific dataset configuration
            dataset_config = database if database != '' else ConfigHandler.__instance['database_config']
            dataset_config_path = '/'.join(config_partial_path \
                + ['{}_config.json'.format(dataset_config.lower())])

            config_specific_file = json.load(open(dataset_config_path, 'r'))
            
            ConfigHandler.__instance['connection']['database'] = config_specific_file['database_name']
            del config_specific_file['database_name']

            ConfigHandler.__instance.update(config_specific_file)
            ConfigHandler.__instance['logging_mode'] = \
                debug_mapping[ConfigHandler.__instance['logging_mode']]
            

        self.__dict__ = ConfigHandler.__instance

    def dump(self):
        config_partial_path = os.path.abspath(__file__).split('/')[:-3] + ['config']
        dataset_config_path = '/'.join(config_partial_path \
            + ['{}_config.json'.format(self.database_config.lower())])

        config_specific_file = json.load(open(dataset_config_path, 'r'))

        config_specific_file['attribute_count'] = self.attribute_count
        config_specific_file['max_ctids_count'] = self.max_ctids_count
        config_specific_file['register_size'] = self.register_size

        with open(dataset_config_path, 'w') as f:
            json.dump(config_specific_file, f, indent=4)

