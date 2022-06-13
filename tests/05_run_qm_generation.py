from pathlib import Path

from pylathedb.utils import ConfigHandler
from pylathedb.lathe import Lathe
from pylathedb.evaluation import EvaluationHandler

config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()


skip_cn_generations = True
weight_schemes= [0,1,2,3]
max_num_query_matches = 20
topk_cns_per_qm_list = [1]
topk_cns = 20
approaches = ['standard']
max_database_accesses = 7
assume_golden_qms = False
input_desired_cn = False
repeat = 1 

for topk_cns_per_qm in topk_cns_per_qm_list: 
    for weight_scheme in weight_schemes:
        output_results_subfolder = f'qm_generation/ws{weight_scheme}/'
        for approach in approaches:
            instance_based_pruning = False if approach == 'standard' else False
            for qsconfig_name,qsconfig_filepath in queryset_configs:

                config = ConfigHandler(reset = True,queryset_config_filepath=qsconfig_filepath)
                lathe = Lathe()
                evaluation_handler = EvaluationHandler()
                lathe.get_queryset()
                evaluation_handler.load_golden_standards()

                print(f"Running queryset {config.queryset_name} with {approach} approach")

                results_filepath = f'{config.results_directory}{output_results_subfolder}{config.queryset_name}-{approach}.json'
                Path(f'{config.results_directory}{output_results_subfolder}').mkdir(parents=True, exist_ok=True)

                results = lathe.run_queryset(
                    skip_cn_generations=skip_cn_generations,
                    weight_scheme=weight_scheme,
                    max_num_query_matches=max_num_query_matches,
                    topk_cns_per_qm = topk_cns_per_qm,
                    topk_cns=topk_cns,
                    instance_based_pruning=instance_based_pruning,
                    max_database_accesses=max_database_accesses,
                    assume_golden_qms = assume_golden_qms,
                    input_desired_cn = input_desired_cn,
                    repeat = repeat,
                    )

                print(f'Saving results in {results_filepath}')
                evaluation_handler.evaluate_results(results,results_filename=results_filepath)