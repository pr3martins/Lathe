from .database_iter import DatabaseIter
from .value_index import ValueIndex
from .schema_index import SchemaIndex
from utils import ConfigHandler,get_logger

logger = get_logger(__name__)
class IndexHandler:
    def __init__(self):
        self.config = ConfigHandler()
        self.value_index = ValueIndex()
        self.schema_index = SchemaIndex()

    def create_indexes(self):
        if not self.config.create_index:
            return

        database_iter = DatabaseIter()
        

        for table,ctid,attribute, word in database_iter:
            self.value_index.add_mapping(word,table,attribute,ctid)
            self.schema_index.increment_frequency(word,table,attribute)

        self.schema_index.process_frequency_metrics()
        num_total_attributes = self.schema_index.get_number_of_attributes()
        self.value_index.process_iaf(num_total_attributes)
        self.schema_index.process_norms_of_attributes(self.value_index.frequencies_iafs())

    def dump_indexes(self,value_index_filename,schema_index_filename):
        self.value_index.persist_to_shelve(value_index_filename)
        self.schema_index.persist_to_shelve(schema_index_filename)

    def load_indexes(self,value_index_filename,schema_index_filename,**kwargs):
        self.value_index = self.value_index.load_from_shelve(value_index_filename,**kwargs)
        self.schema_index = self.schema_index.load_from_shelve(schema_index_filename)
