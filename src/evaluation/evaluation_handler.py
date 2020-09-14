import sys
import os
sys.path.append('../')

from mapper import Mapper
import json
from utils import ConfigHandler
from evaluation.evaluation_item import EvaluationItem

class EvaluationHandler:
    
    def __init__(self):
        self.mapper = Mapper()
        self.config = ConfigHandler()
        self.evaluation_items = []
        self.global_precision = []
        self.global_mrr = 0.0
    
    
    def load_query_file(self):
        json_file = self.config.experiments[self.config.default_evaluation_index]['experiment_file']
        with open(json_file) as f:
            data = json.load(f)    
            for item in data:
                evaluation_item = EvaluationItem()
                evaluation_item.build_from_json(item)
                self.evaluation_items += [evaluation_item]
                
                
    def run_evaluation(self):
        for i, evaluation_item in enumerate(self.evaluation_items):
            ranked_items = self.mapper.get_matches(evaluation_item.segments)
            self.evaluation_items[i].ranked_matches = ranked_items
            self.evaluation_items[i].find_query_match_gt()
            
    def calculate_metrics(self):
        self._get_precision()
        self._get_mrr()        
        
    
    def _get_precision(self):
        positions = [0.0] * 6
        
        for eval_item in self.evaluation_items:
            positions[eval_item.qm_position] += 1.0
        
        precion_metrics = [pos/(len(self.evaluation_items)* 1.0) for pos in positions]
        
        self.global_precision = precion_metrics
        
    def _get_mrr(self):
        mrr = 0.0
        for eval_item in self.evaluation_items:
             mrr += 1/(1.0*eval_item.qm_position) if eval_item.qm_position > 0 else 0.0
        mrr = mrr / (len(self.evaluation_items) * 1.0)
        
        self.global_mrr = mrr
        
        
        