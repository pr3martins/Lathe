import sys

sys.path.append('../')

from utils import ConfigHandler, Similarity
from mapper import Mapper
from evaluation import EvaluationHandler

config = ConfigHandler()
mapper = Mapper()
evaluation_handler = EvaluationHandler()


mapper.load_queryset()
results = mapper.run_queryset()
# results = evaluation_handler.load_results_from_file("../../data/results/results-imdb-002.json")

evaluation_handler.load_golden_standards()

evaluation_handler.evaluate_results(results)