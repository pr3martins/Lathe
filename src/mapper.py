from utils import ConfigHandler, get_logger
from keyword_match import KeywordMatchHandler
from query_match import QueryMatchHandler
from index import IndexHandler
from utils import ConfigHandler, Similarity, get_logger, stopwords
import string
logger = get_logger(__name__)
class Mapper:
    def __init__(self):
        self.config = ConfigHandler()
        self.index_handler = IndexHandler()
       
        self.index_handler.load_indexes(self.config.value_index_filename, self.config.schema_index_filename)
        self.similarity = Similarity(self.index_handler.schema_index)
        self.keyword_match_handler = KeywordMatchHandler(self.similarity)
        self.query_match_handler = QueryMatchHandler()
        
        
       
        
    
    def get_matches(self, segments):
        words = [w.lower().strip(string.punctuation) for s in segments for w in s.replace('"', '').split(' ') if w not in stopwords()]
        kw_schema_matches = self.keyword_match_handler.schema_keyword_match_generator(words, 
            self.index_handler.schema_index)
        kw_schema_matches = self.keyword_match_handler.filter_kwmatches_by_segments(kw_schema_matches,
            segments)

        kw_value_matches = self.keyword_match_handler.value_keyword_match_generator(words, 
            self.index_handler.value_index)
        kw_value_matches = self.keyword_match_handler.filter_kwmatches_by_segments(kw_value_matches,
            segments)

        kw_candidate_query_matches = kw_schema_matches | kw_value_matches
        
        self.query_match_handler.generate_query_matches(words, kw_candidate_query_matches, segments=segments)
        self.query_match_handler.rank_query_matches(self.index_handler.value_index, \
            self.index_handler.schema_index, self.similarity)
        
        return self.query_match_handler.query_matches
        

    def get_candidate_networks(self, segments):
        query_match = self.get_matches(segments)