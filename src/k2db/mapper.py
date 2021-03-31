import json
from timeit import default_timer as timer
from pyspark import SparkContext, SparkConf

from k2db.utils import ConfigHandler, Similarity, get_logger, Tokenizer, next_path
from k2db.index import IndexHandler
from k2db.database import DatabaseHandler
from k2db.keyword_match import KeywordMatchHandler
from k2db.query_match import QueryMatchHandler
from k2db.candidate_network import CandidateNetworkHandler
from k2db.evaluation import EvaluationHandler


logger = get_logger(__name__)
class Mapper:
    def __init__(self, config = None):
        self.config = config
        if self.config is None:
            self.config = ConfigHandler()

        self.database_handler = DatabaseHandler()
        self.index_handler = IndexHandler(database_handler = self.database_handler)

        self.tokenizer = Tokenizer(tokenize_method = 'simple')
        self.index_handler.load_indexes()
        # print(f'SCHEMA GRAPH:\n{self.index_handler.schema_graph.str_edges_info()}')

        self.similarity = Similarity(self.index_handler.schema_index)
        self.keyword_match_handler = KeywordMatchHandler(self.similarity)
        self.query_match_handler = QueryMatchHandler()
        self.evaluation_handler = EvaluationHandler()
        self.candidate_network_handler = CandidateNetworkHandler(database_handler = self.database_handler)
        self.evaluation_handler.load_golden_standards()

    def load_spark(self,**kwargs):
        spark_context = kwargs.get('spark_context',None)
        num_workers = kwargs.get('num_workers',2)
        if spark_context:
            self.spark_context = spark_context
        else:
            self.num_workers = num_workers
            spark_conf = SparkConf().setAppName('K2D').setMaster(f'local[{self.num_workers}]')
            spark_context = SparkContext(conf=spark_conf)
            self.spark_context = spark_context

    def stop_spark(self):
        self.spark_context.stop()

    def load_queryset(self):
        with open(self.config.queryset_filepath,mode='r') as f:
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

        self.index_handler.load_indexes(keywords = keywords_to_load)

        for item in self.queryset:
            keyword_query = item['keyword_query']

            print(f'Running keyword search for query: {keyword_query}')
            result = self.keyword_search(keyword_query,**kwargs)
            results.append(result)

        data = {
            "database":self.config.connection['database'],
            "queryset":self.config.queryset_filepath,
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
        assume_golden_qms = kwargs.get('assume_golden_qms',False)
        max_num_query_matches = kwargs.get('max_num_query_matches',5)
        input_desired_cn = kwargs.get('input_desired_cn',False)

        weight_scheme = kwargs.get('weight_scheme',3)
        #preventing to send multiple values for weight_scheme
        if 'weight_scheme' in kwargs:
            del kwargs['weight_scheme']

        elapsed_time = {
            'km':[],
            'skm':[],
            'vkm':[],
            'qm':[],
            'cn':[],
            'total':[],
        }
        keywords =  self.tokenizer.keywords(keyword_query)

        for _ in range(repeat):
            if not assume_golden_qms:
                start_skm_time = timer()
                sk_matches = self.keyword_match_handler.schema_keyword_match_generator(keywords, self.index_handler.schema_index)
                logger.info('%d SKMs generated: %s',len(sk_matches),sk_matches)

                start_vkm_time = timer()
                vk_matches = self.keyword_match_handler.value_keyword_match_generator(keywords, self.index_handler.value_index)
                # vk_matches = self.keyword_match_handler.filter_kwmatches_by_compound_keywords(vk_matches,compound_keywords)
                logger.info('%d VKMs generated: %s',len(vk_matches),vk_matches)

                kw_matches = sk_matches+vk_matches
                start_qm_time = timer()

                query_matches = self.query_match_handler.generate_query_matches(keywords, kw_matches)
            else:
                start_skm_time = timer()
                start_vkm_time = timer()
                start_qm_time = timer()
                kw_matches = []
                query_matches = self.evaluation_handler.golden_standards[keyword_query]['query_matches']

            ranked_query_matches = self.query_match_handler.rank_query_matches(query_matches,
                self.index_handler.value_index,
                self.index_handler.schema_index,
                self.similarity,
                weight_scheme,
            )

            ranked_query_matches = ranked_query_matches[:max_num_query_matches]
            logger.info('%d QMs generated: %s',len(ranked_query_matches),ranked_query_matches)

            start_cn_time = timer()

            if input_desired_cn:
                desired_cn = self.evaluation_handler.golden_standards[keyword_query]['candidate_networks'][0]
                kwargs['desired_cn'] = desired_cn
            else:
                kwargs['desired_cn'] = None

            ranked_cns = []
            if parallel_cn == False:
                ranked_cns = self.candidate_network_handler.generate_cns(
                    self.index_handler.schema_index,
                    self.index_handler.schema_graph,
                    ranked_query_matches,
                    keywords,
                    weight_scheme,
                     **kwargs,
                )
            else:
                ranked_cns = self.candidate_network_handler.parallelized_generate_cns(
                    self.spark_context,
                    self.index_handler.schema_index,
                    self.index_handler.schema_graph,
                    ranked_query_matches,
                    keywords,
                    weight_scheme,
                    **kwargs,
                )

            logger.info('%d CNs generated: %s',len(ranked_cns),[(cn.score,cn)for cn in ranked_cns])
            end_cn_time = timer()

            elapsed_time['km'].append(   start_qm_time -start_skm_time)
            elapsed_time['skm'].append(  start_vkm_time-start_skm_time)
            elapsed_time['vkm'].append(  start_qm_time -start_vkm_time)
            elapsed_time['qm'].append(   start_cn_time -start_qm_time)
            elapsed_time['cn'].append(   end_cn_time   -start_cn_time)
            elapsed_time['total'].append(end_cn_time   -start_skm_time)

        aggregated_elapsed_time = {phase:min(times) for phase,times in elapsed_time.items()}

        result = {
            'keyword_query':keyword_query,
            'keywords':list(keywords),
            # 'compound_keywords':list(compound_keywords),
            'query_matches':      [query_match.to_json_serializable()
                                  for query_match in ranked_query_matches],
            'candidate_networks': [candidate_network.to_json_serializable()
                                  for candidate_network in ranked_cns],
            'elapsed_time':       aggregated_elapsed_time,
            'num_keyword_matches':len(kw_matches),
            #consider len of unranked query matches
            'num_query_matches':  len(query_matches),
        }

        return result
