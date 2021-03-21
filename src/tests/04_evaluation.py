import sys

sys.path.append('../')

from utils import ConfigHandler, Similarity
from mapper import Mapper
from evaluation import EvaluationHandler


config = ConfigHandler()
mapper = Mapper()
evaluation_handler = EvaluationHandler()


mapper.load_queryset()
results = mapper.run_queryset(prepruning=True)

evaluation_handler.load_golden_standards()
evaluation_handler.evaluate_results(results)
