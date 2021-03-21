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

class ConfigHandler:
    __instance = None
    def __init__(self,reset=False,**kwargs):
        if ConfigHandler.__instance is None or reset:
            general_config_file = kwargs.get('general_config_file','../../config/config.json')            
            config = self.load_config(general_config_file)     

            config['logging_mode'] = debug_mapping[config['logging_mode']]       

            if 'queryset_config_file' in kwargs:
                config['queryset_config_file'] = kwargs['queryset_config_file']
            queryset_config = self.load_config(config['queryset_config_file'])

            if 'dataset_config_file' in kwargs:
                queryset_config['dataset_config_file']= kwargs['dataset_config_file']
            dataset_config = self.load_config(queryset_config['dataset_config_file'])

            config['connection']['database'] = dataset_config['database']
            del dataset_config['database']

            ConfigHandler.__instance = config
            ConfigHandler.__instance.update(queryset_config)
            ConfigHandler.__instance.update(dataset_config)

        self.__dict__ = ConfigHandler.__instance

    def load_config(self,filename):
        config_file = open(filename, 'r')
        config = json.load(config_file)
        return config
    
    def set_logging_level(self,ĺevel):
        self.logging_mode = debug_mapping[ĺevel]
        ConfigHandler.__instance['logging_mode']=self.logging_mode
