import json
import string

from utils import ConfigHandler, Similarity, get_logger, Tokenizer,timestr
from index import IndexHandler
from keyword_match import KeywordMatchHandler
from query_match import QueryMatchHandler
from candidate_network import CandidateNetwork
from evaluation import EvaluationHandler


logger = get_logger(__name__)
class Mapper:
    def __init__(self):
        self.config = ConfigHandler()
        self.index_handler = IndexHandler()

        self.tokenizer = Tokenizer(tokenize_method = 'simple')
        self.index_handler.load_indexes(self.config.value_index_filename, self.config.schema_index_filename)
        self.similarity = Similarity(self.index_handler.schema_index)
        self.keyword_match_handler = KeywordMatchHandler(self.similarity)
        self.query_match_handler = QueryMatchHandler()
        self.evaluation_handler = EvaluationHandler()


    def load_queryset(self):
        with open(self.config.queryset_file,mode='r') as f:
            queryset = json.load(f)
        self.queryset = queryset

    def run_queryset(self,**kwargs):
        '''
        results_filename is declared here for sake of readability. But the
        default value is only set later to get a timestamp more accurate.
        '''
        results_filename = kwargs.get('results_filename',None)

        results =[]
        for item in self.queryset:
            keyword_query = item['keyword_query']
            print(f'Running keyword search for query: {keyword_query}')
            result = self.keyword_search(keyword_query)
            results.append(result)

        data = {
            "database":self.config.connection['database'],
            "queryset":self.config.queryset_file,
            "results":results,
        }

        if results_filename is None:
            results_filename = f'{self.config.results_directory}results-{self.config.database_config}-{timestr()}.json'

        with open(results_filename,mode='w') as f:
            logger.info(f'Writing results in {results_filename}')
            json.dump(data,f)

        return data


    def keyword_search(self,keyword_query):
        keywords =  self.tokenizer.keywords(keyword_query)
        compound_keywords =  self.tokenizer.compound_keywords(keyword_query)

        sk_matches = self.keyword_match_handler.schema_keyword_match_generator(keywords, self.index_handler.schema_index)

        vk_matches = self.keyword_match_handler.value_keyword_match_generator(keywords, self.index_handler.value_index)
        vk_matches = self.keyword_match_handler.filter_kwmatches_by_compound_keywords(vk_matches,compound_keywords)

        kw_matches = sk_matches | vk_matches

        query_matches = self.query_match_handler.generate_query_matches(keywords, kw_matches)

        self.query_match_handler.rank_query_matches(query_matches,
            self.index_handler.value_index,
            self.index_handler.schema_index,
            self.similarity)

        #TODO Add candidate networks

        result = {
            'keyword_query':keyword_query,
            'keywords':keywords,
            'compound_keywords':compound_keywords,
            'query_matches':[query_match.to_json_serializable()
                             for query_match in query_matches[:10]],
        }

        return result
