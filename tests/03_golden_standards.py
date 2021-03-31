from k2db.evaluation import EvaluationHandler
from k2db.utils import ConfigHandler
from k2db.index import IndexHandler
from k2db.database import DatabaseHandler

config = ConfigHandler()
queryset_configs = config.get_queryset_configs()
queryset_configs.sort()

for name,filepath in queryset_configs:
    print(f'Queryset:{name}')
    config = ConfigHandler(reset=True,queryset_config_filepath=filepath)
    evaluation_handler = EvaluationHandler()
    evaluation_handler.load_golden_standards()

    index_handler = IndexHandler()
    index_handler.load_indexes()

    database_handler = DatabaseHandler()

    for i,item in enumerate(evaluation_handler.golden_standards.values()):
        keyword_query = item['keyword_query']
        qm = item['query_matches'][0]
        cn = item['candidate_networks'][0]
        sql = cn.get_sql_from_cn(index_handler.schema_graph,show_evaluation_fields = True)

        print(f'{i+1:02} Keyword Query: {keyword_query}')
        print(f'Query Match:\n{qm}')
        print(f'Candidate Network:\n{cn}')
        print(f'SQL:\n{sql}')
        results = database_handler.exec_sql(sql,show_results=False)

        fields = ['0.tuples']
        if '1.relationships' in results._field_names:
            fields.append('1.relationships')
        print(results.get_string(fields=fields))         
        
