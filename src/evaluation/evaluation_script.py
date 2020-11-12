import sys
import os
sys.path.append('../')
from evaluation import EvaluationHandler

evaluation = EvaluationHandler()
evaluation.load_query_file()
evaluation.run_evaluation()
evaluation.calculate_metrics()
evaluation.save_gt()