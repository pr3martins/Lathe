from k2db.utils import ConfigHandler, next_path
from k2db.mapper import Mapper
from k2db.evaluation import EvaluationHandler

approaches = ['standard','pospruning','prepruning']


config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()

instance_based_approaches_parallel_cn = True
output_results_subfolder = 'weight_scheme_3/'
weight_scheme=3

spark_context = None

for approach in approaches:
    prepruning = (approach == 'prepruning')
    pospruning = (approach == 'pospruning')
    if approach == 'standard':
        repeat = 10
        parallel_cn = False        
    else:
        repeat = 1 
        parallel_cn = instance_based_approaches_parallel_cn
    
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

        results_filepath = next_path(f'{config.results_directory}{output_results_subfolder}{config.queryset_name}-{approach}-%03d.json')

        results = mapper.run_queryset(
            parallel_cn= parallel_cn,
            repeat = repeat,
            prepruning=prepruning,
            pospruning=pospruning,
            weight_scheme=weight_scheme
            )

        
        print(f'Saving results in {results_filepath}')
        evaluation_handler.evaluate_results(results,results_filename=results_filepath)
