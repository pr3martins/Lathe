import json
import string
from timeit import default_timer as timer


from utils import ConfigHandler, Similarity, get_logger, Tokenizer,timestr, next_path
from index import IndexHandler
from keyword_match import KeywordMatchHandler
from query_match import QueryMatchHandler
from candidate_network import CandidateNetworkHandler
from evaluation import EvaluationHandler


logger = get_logger(__name__)
class Mapper:
    def __init__(self):
        self.config = ConfigHandler()
        self.index_handler = IndexHandler()

        self.tokenizer = Tokenizer(tokenize_method = 'simple')
        self.index_handler.load_indexes(self.config.value_index_filename,
            self.config.schema_index_filename
        )
        self.index_handler.create_schema_graph()
        self.similarity = Similarity(self.index_handler.schema_index)
        self.keyword_match_handler = KeywordMatchHandler(self.similarity)
        self.query_match_handler = QueryMatchHandler()
        self.evaluation_handler = EvaluationHandler()
        self.candidate_network_handler = CandidateNetworkHandler()


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

        keywords_to_load = {keyword for item in self.queryset for keyword in set(self.tokenizer.keywords(item['keyword_query']))}

        self.index_handler.load_indexes(self.config.value_index_filename,
            self.config.schema_index_filename,
            keywords = keywords_to_load
        )

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
            results_filename = next_path(f'{self.config.results_directory}results-{self.config.database_config}-%03d.json')

        with open(results_filename,mode='w') as f:
            logger.info(f'Writing results in {results_filename}')
            json.dump(data,f, indent = 4)

        return data


    def keyword_search(self,keyword_query):
        
        start_kws_time = timer()

        keywords =  set(self.tokenizer.keywords(keyword_query))
        compound_keywords =  set(self.tokenizer.compound_keywords(keyword_query))

        print(f'Keywords: {keywords}\nCompound Keywords: {compound_keywords}')

        sk_matches = self.keyword_match_handler.schema_keyword_match_generator(keywords, self.index_handler.schema_index)

        print(f'SK Matches: {sk_matches}')

        vk_matches = self.keyword_match_handler.value_keyword_match_generator(keywords, self.index_handler.value_index)
        vk_matches = self.keyword_match_handler.filter_kwmatches_by_compound_keywords(vk_matches,compound_keywords)

        print(f'VK Matches: {vk_matches}')

        kw_matches = sk_matches | vk_matches

        keywords_from_kw_matches = {tuple(sorted(keyword_match.keywords())) for keyword_match in kw_matches}

        print(f'KW from K Matches: {keywords_from_kw_matches}')

        print(f'Num Keyword Matches: {len(kw_matches)}')
        print(f'Num Keyword Matches: {len(keywords_from_kw_matches)}')
        
        start_qm_time = timer()

        query_matches = self.query_match_handler.generate_query_matches(keywords, kw_matches)

        print(f'{len(query_matches)} QMs generated')

        ranked_query_matches = self.query_match_handler.rank_query_matches(query_matches,
            self.index_handler.value_index,
            self.index_handler.schema_index,
            self.similarity)

        ranked_query_matches = ranked_query_matches[:10]

        print(f'Ranked Query Matches: {ranked_query_matches}')

        start_cn_time = timer()
        schema_graph = self.index_handler.schema_graph
        ranked_cns = self.candidate_network_handler.generate_cns(self.index_handler.schema_index,schema_graph,query_matches)

        end_cn_time = timer()

        for i,(candidate_network) in enumerate(ranked_cns[:3]):
            print(f'{i+1} CN:\n{candidate_network}\n')
        print('\n')


        result = {
            'keyword_query':keyword_query,
            'keywords':list(keywords),
            'compound_keywords':list(compound_keywords),
            'query_matches':[query_match.to_json_serializable()
                             for query_match in query_matches],
            'candidate_networks': [candidate_network.to_json_serializable()
                                   for candidate_network in ranked_cns],
            'elapsed_time': {
                'km':start_qm_time-start_kws_time,
                'qm':start_cn_time-start_qm_time,
                'cn':end_cn_time-start_cn_time,
                'total':end_cn_time-start_kws_time,
            }
        }

        return result
