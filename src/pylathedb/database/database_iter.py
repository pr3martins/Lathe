import string
import sys
import re
import psycopg2
from psycopg2.sql import SQL, Identifier

from pylathedb.utils import ConfigHandler, get_logger, Tokenizer

logger = get_logger(__name__)
class DatabaseIter:
    def __init__(self,config,database_table_columns, **kwargs):
        self.limit_per_table = kwargs.get('limit_per_table', None)
        self.tokenizer = kwargs.get('tokenizer', Tokenizer())
        self.config = config
        self.database_table_columns=database_table_columns
        

        self.table_hash = self._get_indexable_schema_elements()



    def _schema_element_validator(self,table,column):
        return True

    def _get_indexable_schema_elements(self):
         with psycopg2.connect(**self.config.connection) as conn:
            with conn.cursor() as cur:
                GET_TABLE_AND_COLUMNS_WITHOUT_FOREIGN_KEYS_SQL='''
                    SELECT
                        c.table_name,
                        c.column_name
                    FROM
                        information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                          AND tc.constraint_type = 'FOREIGN KEY'
                        RIGHT JOIN information_schema.columns AS c
                          ON c.table_name=tc.table_name
                          AND c.column_name = kcu.column_name
                          AND c.table_schema = kcu.table_schema
                    WHERE
                        c.table_schema='public'
                        AND tc.constraint_name IS NULL;
                '''
                cur.execute(GET_TABLE_AND_COLUMNS_WITHOUT_FOREIGN_KEYS_SQL)
                table_hash = {}
                for table,column in cur.fetchall():
                    if (table,column) not in self.database_table_columns:
                        print(f'SKIPPED {(table,column)}')
                        continue
                    table_hash.setdefault(table,[]).append(column)
                return table_hash

    def __iter__(self):
        for table,columns in self.table_hash.items():

            indexable_columns = [col for col in columns if self._schema_element_validator(table,col)]

            if len(indexable_columns)==0:
                continue

            print('\nINDEXING {}({})'.format(table,', '.join(indexable_columns)))

            with psycopg2.connect(**self.config.connection) as conn:
                with conn.cursor() as cur:
                    '''
                    NOTE: Table and columns can't be directly passed as parameters.
                    Thus, the psycopg2.sql.SQL command with psycopg2.sql.Identifiers is used
                    '''

                    if self.limit_per_table is not None:
                        text = (f"SELECT ctid, {{}} FROM {{}} LIMIT {self.limit_per_table};")
                    else:
                        text = ("SELECT ctid, {} FROM {};")

                    cur.execute(
                        SQL(text)
                        .format(SQL(', ').join(Identifier(col) for col in indexable_columns),
                                Identifier(table))
                            )

                    arraysize = 100000
                    data = cur.fetchmany(arraysize)
                    while len(data) != 0:
                        for row in data:
                            ctid = row[0]
                            for col in range(1,len(row)):
                                column = cur.description[col][0]
                                text = str(row[col])
                                tokens = self.tokenizer.tokenize(text)

                                for word in tokens:
                                    #print(tokens)
                                    # if len([ch for ch in word if ch in string.punctuation]) != 0 and not is_url:
                                    #     print("Tokenizer not working {}".format(word, row[col]))
                                    #     sys.exit()
                                    yield table,ctid,column, word

                        data = cur.fetchmany(arraysize)
