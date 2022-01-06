from pathlib import Path

from pylathedb.utils import ConfigHandler
from pylathedb.lathe import Lathe
from pylathedb.evaluation import EvaluationHandler

config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort(key = lambda config:(1 if '_clear_intents' in config[0] else 0,config))
for x in queryset_configs:
    print(x)

skip_cn_generations = False
weight_scheme= 0
max_num_query_matches = 99999999
topk_cns = 99999999
approaches = {
    'standard':[99999999],
}
max_database_accesses = 99999999
assume_golden_qms = False
input_desired_cn = False
repeat = 1

write_evaluation_only=True
skip_ranking_evaluation=True

for approach in approaches:
    # output_results_subfolder = f'nun_cns/'
    for topk_cns_per_qm in approaches[approach]:     
        output_results_subfolder = f'num_cns/'

        instance_based_pruning = (approach != 'standard')
        for qsconfig_name,qsconfig_filepath in queryset_configs:
            if '_clear_intents' not in qsconfig_name:         
                preprocessed_results = {}
            else:
                preprocessed_results = {}

            config = ConfigHandler(reset = True,queryset_config_filepath=qsconfig_filepath)
            lathe = Lathe()
            evaluation_handler = EvaluationHandler()
            lathe.get_queryset()                
            evaluation_handler.load_golden_standards()

            print(f"Running queryset {config.queryset_name} with {approach} approach")

            results_filepath = f'{config.results_directory}{output_results_subfolder}{config.queryset_name}-{approach}.json'
            Path(f'{config.results_directory}{output_results_subfolder}').mkdir(parents=True, exist_ok=True)

            import os.path
            if os.path.isfile(results_filepath):
                print(f'Skipped {results_filepath}')
                continue

            results = lathe.run_queryset(
                preprocessed_results = preprocessed_results,
                weight_scheme=weight_scheme,
                max_num_query_matches=max_num_query_matches,
                topk_cns_per_qm = topk_cns_per_qm,
                topk_cns=topk_cns,
                instance_based_pruning=instance_based_pruning,
                assume_golden_qms = assume_golden_qms,
                input_desired_cn = input_desired_cn,
                repeat = repeat,
                max_database_accesses=max_database_accesses,
                )

            print(f'Saving results in {results_filepath}')
            evaluation_handler.evaluate_results(
                results,
                results_filename=results_filepath,
                write_evaluation_only=write_evaluation_only,
                skip_ranking_evaluation=skip_ranking_evaluation,
            )