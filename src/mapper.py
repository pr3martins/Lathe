import json
import string
from timeit import default_timer as timer
from pyspark import SparkContext, SparkConf
from itertools import chain


from utils import ConfigHandler, Similarity, get_logger, Tokenizer,timestr, next_path
from index import IndexHandler
from database import DatabaseHandler
from keyword_match import KeywordMatchHandler
from query_match import QueryMatchHandler
from candidate_network import CandidateNetworkHandler
from evaluation import EvaluationHandler


logger = get_logger(__name__)
class Mapper:
    def __init__(self):
        self.config = ConfigHandler()
        self.database_handler = DatabaseHandler()
        self.index_handler = IndexHandler(database_handler = self.database_handler)

        self.tokenizer = Tokenizer(tokenize_method = 'simple')
        self.index_handler.load_indexes(self.config.value_index_filename,
            self.config.schema_index_filename
        )
        self.index_handler.create_schema_graph()
        self.similarity = Similarity(self.index_handler.schema_index)
        self.keyword_match_handler = KeywordMatchHandler(self.similarity)
        self.query_match_handler = QueryMatchHandler()
        self.evaluation_handler = EvaluationHandler()
        self.candidate_network_handler = CandidateNetworkHandler(database_handler = self.database_handler)

    def load_spark(self,num_workers):
        self.num_workers = num_workers
        conf = SparkConf().setAppName('K2D').setMaster(f'local[{self.num_workers}]')
        spark_context = SparkContext(conf=conf)
        self.spark_context = spark_context

    def stop_spark(self):
        self.spark_context.stop()

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
        export_results = kwargs.get('export_results',False)
        approach = kwargs.get('approach','standard')

        results =[]

        keywords_to_load = {keyword for item in self.queryset for keyword in set(self.tokenizer.keywords(item['keyword_query']))}

        self.index_handler.load_indexes(self.config.value_index_filename,
            self.config.schema_index_filename,
            keywords = keywords_to_load
        )

        for item in self.queryset:
            keyword_query = item['keyword_query']

            print(f'Running keyword search for query: {keyword_query}')
            result = self.keyword_search(keyword_query,**kwargs)
            results.append(result)

        data = {
            "database":self.config.connection['database'],
            "queryset":self.config.queryset_file,
            "results":results,
        }
        
        if export_results:
            if results_filename is None:
                results_filename = next_path(f'{self.config.results_directory}{self.config.queryset_name}-{approach}-%03d.json')

            with open(results_filename,mode='w') as f:
                logger.info(f'Writing results in {results_filename}')
                json.dump(data,f, indent = 4)

        return data


    def keyword_search(self,keyword_query,**kwargs):
        parallel_cn = kwargs.get('parallel_cn', False)
        repeat = kwargs.get('repeat',1)
        weight_scheme = kwargs.get('weight_scheme',1)
        
        elapsed_time = {
            'km':[],
            'qm':[],
            'cn':[],
            'total':[],
        }

        for iteration in range(repeat):
            start_km_time = timer()

            keywords =  set(self.tokenizer.keywords(keyword_query))
            
            # compound_keywords =  set(self.tokenizer.compound_keywords(keyword_query))
            # printf(f'Compound Keywords: {compound_keywords}')

            sk_matches = self.keyword_match_handler.schema_keyword_match_generator(keywords, self.index_handler.schema_index)

            vk_matches = self.keyword_match_handler.value_keyword_match_generator(keywords, self.index_handler.value_index)
            # vk_matches = self.keyword_match_handler.filter_kwmatches_by_compound_keywords(vk_matches,compound_keywords)


            kw_matches = sk_matches+vk_matches
            

            # keywords_from_kw_matches = {tuple(sorted(keyword_match.keywords())) for keyword_match in kw_matches}

            # print(f'KW from K Matches: {keywords_from_kw_matches}')        
            
            start_qm_time = timer()

            query_matches = self.query_match_handler.generate_query_matches(keywords, kw_matches)

            ranked_query_matches = self.query_match_handler.rank_query_matches(query_matches,
                self.index_handler.value_index,
                self.index_handler.schema_index,
                self.similarity,
                weight_scheme,
            )

            ranked_query_matches = ranked_query_matches[:10] 

            start_cn_time = timer()
            schema_graph = self.index_handler.schema_graph

            # ranked_cns = []
            if parallel_cn == False:
                ranked_cns = self.candidate_network_handler.generate_cns(self.index_handler.schema_index,schema_graph,query_matches, **kwargs)
            else:
                ranked_cns = self.candidate_network_handler.parallelized_generate_cns(self.spark_context, self.index_handler.schema_index, schema_graph, query_matches, **kwargs)
            

            end_cn_time = timer()

            elapsed_time['km'].append(   start_qm_time-start_km_time)
            elapsed_time['qm'].append(   start_cn_time-start_qm_time)
            elapsed_time['cn'].append(   end_cn_time  -start_cn_time)
            elapsed_time['total'].append(end_cn_time  -start_km_time)

        aggregated_elapsed_time = {phase:min(times) for phase,times in elapsed_time.items()}

        result = {
            'keyword_query':keyword_query,
            'keywords':list(keywords),
            # 'compound_keywords':list(compound_keywords),
            'query_matches':[query_match.to_json_serializable()
                             for query_match in query_matches],
            'candidate_networks': [candidate_network.to_json_serializable()
                                   for candidate_network in ranked_cns],
            'elapsed_time': aggregated_elapsed_time,
            'num_keyword_matches': len(kw_matches),
            'num_query_matches': len(query_matches),
        }

        return result
