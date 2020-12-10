from copy import deepcopy
import json
import sys
from collections import OrderedDict

from utils import ConfigHandler,get_logger,timestr
from query_match import QueryMatch
from candidate_network import CandidateNetwork

logger = get_logger(__name__)

class EvaluationHandler:

    def __init__(self):
        self.config = ConfigHandler()


    def load_golden_standards(self):
        with open(self.config.golden_standards_file,mode='r') as f:
            data = json.load(f)

        golden_standards = OrderedDict()
        for i, item in enumerate(data):

            if 'query_matches' in item:
                item['query_matches'] = [QueryMatch.from_json_serializable(json_serializable_qm)
                                         for json_serializable_qm in item['query_matches']]

            if 'candidate_networks' in item:
                item['candidate_networks'] = [CandidateNetwork.from_json_serializable(json_serializable_cn)
                                         for json_serializable_cn in item['candidate_networks']]

            golden_standards[item['keyword_query']] = item

        self.golden_standards = golden_standards

    def load_results_from_file(self,filename):
        with open(filename,mode='r') as f:
            data = json.load(f)
        return data


    def evaluate_results(self,results, **kwargs):
        '''
        results_filename is declared here for sake of readability. But the
        default value is only set later to get a timestamp more accurate.
        '''
        results_filename = kwargs.get('results_filename',None)

        self.evaluate_query_matches(results)

        if results_filename is None:
            results_filename = f'{self.config.results_directory}evaluated-results-{self.config.database_config}-{timestr()}.json'
            with open(results_filename,mode='w') as f:
                logger.info(f'Writing evaluated results in {results_filename}')
                json.dump(results,f)

    def evaluate_query_matches(self,results,**kwargs):
        # TODO decide a better name for results
        max_k = kwargs.get('max_k',10)

        results.setdefault('evaluation',{})
        results['evaluation']['query_matches']={}

        relevant_positions = []
        for item in results['results']:

            if 'query_matches' in item:
                golden_qm = self.golden_standards[item['keyword_query']]['query_matches'][0]

                query_matches = [QueryMatch.from_json_serializable(json_serializable_qm)
                                 for json_serializable_qm in item['query_matches']]

                relevant_position = self.get_relevant_position(query_matches,golden_qm)

                relevant_positions.append(relevant_position)

        results['evaluation']['query_matches']['mrr'] = self.get_mean_reciprocal_rank(relevant_positions)

        precision_at_k = {f'p@{k}' : self.get_precision_at_k(k,relevant_positions)
                          for k in range(1,max_k+1)}
        results['evaluation']['query_matches'].update(precision_at_k)

        results['evaluation']['query_matches']['relevant_positions']=relevant_positions

        print(f'evaluation {results["evaluation"]}')

    def get_relevant_position(self,items,ground_truth):
        for i,item in enumerate(items):
            if item==ground_truth:
                return (i+1)
        return -1

    def get_mean_reciprocal_rank(self,relevant_positions):
        if len(relevant_positions)==0:
            return 0
        sum = 0
        for relevant_position in relevant_positions:
            if relevant_position != -1:
                reciprocal_rank = 1/relevant_position
            else:
                reciprocal_rank = 0
            sum += reciprocal_rank

        mrr = sum/len(relevant_positions)
        return mrr

    def get_precision_at_k(self,k,relevant_positions):
        if len(relevant_positions)==0:
            return 0
        sum = 0
        for relevant_position in relevant_positions:
            if relevant_position <= k and relevant_position!=-1:
                sum+=1
        presition_at_k = sum/len(relevant_positions)
        return presition_at_k
