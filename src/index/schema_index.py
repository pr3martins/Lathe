from utils import ConfigHandler,get_logger


class SchemaIndex:
    def __init__(self):
        self.config = ConfigHandler()
        self.attributes = {}
        self.tables = {}
        self.attribute_file = open(self.config.attribute_file, 'a+b')
        self.max_elements = ['', '']


    def add(self, table, attribute):
        key = '{}.{}'.format(table, attribute)
        self.tables.setdefault(table, len(self.tables.keys()))
        return self.attributes.setdefault(key, len(self.attributes.keys()))    
    

    
