from pathlib import Path
from pylathedb.utils import ConfigHandler
from pylathedb.lathe import Lathe
from pylathedb.evaluation import EvaluationHandler

config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()

skip_cn_generations = False
weight_scheme= 0
max_num_query_matches = 1
topk_cns_per_qm_list = [10]
topk_cns = 10
approaches = ['standard','ipruning']
max_database_accesses = 9
assume_golden_qms = True
input_desired_cn = False
repeat = 1 

for topk_cns_per_qm in topk_cns_per_qm_list: 
    output_results_subfolder = f'cns_from_golden_qms/'
    for approach in approaches:
        instance_based_pruning = False if approach == 'standard' else True
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