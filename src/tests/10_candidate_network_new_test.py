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

# imdb
# results = evaluation_handler.load_results_from_file("../../data/results/results-imdb-20201202-185753.json")

# imdb clear_intents
# results = evaluation_handler.load_results_from_file("../../data/results/results-imdb_clear-20201202-190137.json")

# imdb
# results = evaluation_handler.load_results_from_file("../../data/results/results-20201202-182903.json")

# evaluation_handler.load_golden_standards()
#
# evaluation_handler.evaluate_results(results)
