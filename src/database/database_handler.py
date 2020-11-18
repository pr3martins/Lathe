import psycopg2

from utils import ConfigHandler,get_logger

from .database_iter import DatabaseIter



logger = get_logger(__name__)

class DatabaseHandler:
    def __init__(self):
        self.config = ConfigHandler()

        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(\
            self.config.connection['host'], self.config.connection['database'], \
            self.config.connection['user'], self.config.connection['password'])

    def get_tables_and_attributes(self):
        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                sql = '''
                        SELECT table_name,column_name
                        FROM information_schema.columns
                        WHERE table_schema='public'
                        ORDER by 1,2;
                        '''
                cur.execute(sql)
                return cur.fetchall()

    def iterate_over_keywords(self,schema_index,**kwargs):
        database_table_columns=schema_index.tables_attributes()
        return DatabaseIter(database_table_columns,**kwargs)

    def get_fk_constraints(self):
        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                sql = '''
                        SELECT conname AS constraint_name,
                           conrelid::regclass AS table_name,
                           ta.attname AS column_name,
                           confrelid::regclass AS foreign_table_name,
                           fa.attname AS foreign_column_name
                        FROM (
                            SELECT conname, conrelid, confrelid,
                                  unnest(conkey) AS conkey, unnest(confkey) AS confkey
                             FROM pg_constraint
                             WHERE contype = 'f'
                        ) sub
                        JOIN pg_attribute AS ta ON ta.attrelid = conrelid AND ta.attnum = conkey
                        JOIN pg_attribute AS fa ON fa.attrelid = confrelid AND fa.attnum = confkey
                        ORDER BY 1,2,4;
                    '''
                cur.execute(sql)
                return cur.fetchall()

    def exec_sql (conn_string,SQL,**kwargs):
        show_results=kwargs.get('show_results',True)

        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(SQL)

                    if cur.description:
                        table = PrettyTable()
                        table.field_names = [ '{}{}'.format(i,col[0]) for i,col in enumerate(cur.description)]
                        for row in cur.fetchall():
                            table.add_row(row)
                    if show_results:
                        print(table)
                except:
                    print('ERRO SQL:\n',SQL)
                    raise
                return cur.rowcount>0
