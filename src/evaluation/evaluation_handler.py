from mapper import Mapper
from copy import deepcopy
import json
import sys

from utils import ConfigHandler
from evaluation.evaluation_item import EvaluationItem


class EvaluationHandler:
    
    def __init__(self):
        self.mapper = Mapper()
        self.config = ConfigHandler()
        self.evaluation_items = []
        self.global_precision = []
        self.global_mrr = 0.0
        self.start_from = 0
    
    
    def load_query_file(self):
        json_file = self.config.evaluation[self.config.default_evaluation_index]['query_file']
        with open(json_file) as f:
            data = json.load(f)    
            for i, item in enumerate(data):
                evaluation_item = EvaluationItem()
                evaluation_item.build_from_json(item)
                self.evaluation_items += [evaluation_item]
        self.start_from = self.config.evaluation[self.config.default_evaluation_index]['start_from']
        self.result_file =  self.config.evaluation[self.config.default_evaluation_index]['result_file']
                 
    def run_evaluation(self):
        
        for i, evaluation_item in enumerate(self.evaluation_items):
            if i < self.start_from:
                continue

            print(f"running for {evaluation_item.segments}")
            ranked_items = self.mapper.get_matches(evaluation_item.segments)
            max_items = 30 if len(ranked_items) > 30 else len(ranked_items)
            self.evaluation_items[i].ranked_matches = self._fix_matches(ranked_items[:max_items])
            self.evaluation_items[i].find_query_match_gt()
            self.store_partial_result(self.evaluation_items[i])

            
    def calculate_metrics(self):
        self._get_precision()
        self._get_mrr()        
        
    
    def _get_precision(self):
        positions = [0.0] * 6
        for eval_item in self.evaluation_items:
            pos = 5 if eval_item.qm_position == -1 or eval_item.qm_position >= 5 else eval_item.qm_position
            positions[pos] += 1.0
        
        precion_metrics = [pos/(len(self.evaluation_items)* 1.0) for pos in positions]
        
        self.global_precision = precion_metrics
        
    def _get_mrr(self):
        mrr = 0.0
        for eval_item in self.evaluation_items:
             mrr += 1/(1.0*eval_item.qm_position) if eval_item.qm_position > 0 else 0.0
        mrr = mrr / (len(self.evaluation_items) * 1.0)
        
        self.global_mrr = mrr

    def save_gt(self):
        print(self.global_mrr, self.global_precision)

    def store_partial_result(self, eval_item):
        with open(self.result_file, 'a+') as f:
            result = '[{}]'.format(','.join([x.to_json() for x in eval_item.ranked_matches]))
            match_item = {'query_candidate_matches': []}
            match_item['query_candidate_matches'] = json.loads(result)
            match_item['query_candidate_item'] = eval_item.qm_position
            match_item['query_id'] = eval_item.id

            f.write('{}\n'.format(json.dumps(match_item)))

    def _get_default_attributes(self):
        attribute_map={}
        with open(self.config.relations_file, "r") as f:
            attribute_map = {item['name']:attr['name']  for item in json.load(f) \
                for attr in item['attributes'] if attr.get('importance','') == 'primary' }
        
        return attribute_map

    def _fix_matches(self, ranked_matches):
        attribute_map = self._get_default_attributes()
        for i, ranked_match in enumerate(ranked_matches): 
            for kw_match in ranked_match.matches:
                if kw_match.has_default_mapping() and kw_match.table in attribute_map:
                    kw_match.replace_default_mapping(attribute_map[kw_match.table])
        
        return ranked_matches
                
            


        
        