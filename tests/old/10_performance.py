import sys

sys.path.append('../')

from utils import ConfigHandler, Similarity
from mapper import Mapper
from evaluation import EvaluationHandler

config = ConfigHandler()
mapper = Mapper()
evaluation_handler = EvaluationHandler()

mapper.load_queryset()

parallel_cn = False
if parallel_cn:
    mapper.load_spark(2)

results = mapper.run_queryset(parallel_cn=parallel_cn,repeat = 10,prepruning=False)
# results = evaluation_handler.load_results_from_file("../../data/results/results-imdb-002.json")

evaluation_handler.load_golden_standards()

evaluation_handler.evaluate_results(results)