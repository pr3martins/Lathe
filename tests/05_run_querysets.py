from pathlib import Path

from k2db.utils import ConfigHandler
from k2db.mapper import Mapper
from k2db.evaluation import EvaluationHandler

approaches = ['standard','pospruning']
# approaches = ['standard','pospruning','prepruning']


config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()

instance_based_approaches_parallel_cn = True

spark_context = None
assume_golden_qms = True
input_desired_cn = True
topk_cns_per_qm_list = [10]
# output_results_subfolder = 'weight_scheme_3/
# weight_scheme=3
max_num_query_matches = 5

repeat = 1
parallel_cn = False        

for topk_cns_per_qm in topk_cns_per_qm_list:
    for weight_scheme in range(1): #go back to 4
        # output_results_subfolder = f'{topk_cns_per_qm}cns_per_qm/ws{weight_scheme}/'
        output_results_subfolder = f'assume_gold_qms/'
        Path(f'{config.results_directory}{output_results_subfolder}').mkdir(parents=True, exist_ok=True)

        for approach in approaches:
            prepruning = (approach == 'prepruning')
            pospruning = (approach == 'pospruning')
            # if approach == 'standard':
            #     repeat = 10
            #     parallel_cn = False        
            # else:
            #     repeat = 1 
            #     parallel_cn = instance_based_approaches_parallel_cn
            
            for qsconfig_name,qsconfig_filepath in queryset_configs:

                config = ConfigHandler(reset = True,queryset_config_filepath=qsconfig_filepath)
                mapper = Mapper()
                evaluation_handler = EvaluationHandler()
                mapper.load_queryset()

                if parallel_cn:
                    mapper.load_spark(spark_context = spark_context,num_workers=2)
                    spark_context = mapper.spark_context

                evaluation_handler.load_golden_standards()

                print(f'Running queryset {config.queryset_name} with {approach} approach')

                results_filepath = f'{config.results_directory}{output_results_subfolder}{config.queryset_name}-{approach}.json'

                results = mapper.run_queryset(
                    parallel_cn= parallel_cn,
                    repeat = repeat,
                    prepruning=prepruning,
                    pospruning=pospruning,
                    weight_scheme=weight_scheme,
                    assume_golden_qms = assume_golden_qms,
                    topk_cns_per_qm = topk_cns_per_qm,
                    input_desired_cn = input_desired_cn,
                    max_num_query_matches=max_num_query_matches,
                    )

                
                print(f'Saving results in {results_filepath}')
                evaluation_handler.evaluate_results(results,results_filename=results_filepath)
