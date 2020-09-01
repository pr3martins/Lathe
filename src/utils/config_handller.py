import json
import os

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
            config_path = '/'.join(__file__.split('/')[:-1] + ['config.json'])
            config_file = open(config_path, 'r')             
            ConfigHandler.__instance = json.load(config_file)
            ConfigHandler.__instance.setdefault('table_file', None)
            
            #getting the specific dataset configuration
            dataset_config = database if database != '' else ConfigHandler.__instance['databaseConfig']
            dataset_config_path = '/'.join(__file__.split('/')[:-1] \
                + ['{}_config.json'.format(dataset_config.lower())])

            config_specfic_file = open(dataset_config_path, 'r') 
            ConfigHandler.__instance['connection']['database'] = config_database_file['database_name']
            del config_database_file['database_name']
            
            ConfigHandler.__instance.update(json.load(config_database_file))
            ConfigHandler.__instance['logging_mode'] = \
                debug_mapping[ConfigHandler.__instance['logging_mode']]
            print(ConfigHandler.__instance)

        self.__dict__ = ConfigHandler.__instance
        
