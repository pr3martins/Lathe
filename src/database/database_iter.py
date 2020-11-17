import string
import sys
import re
import psycopg2
from psycopg2 import sql

from utils import ConfigHandler, get_logger, stopwords

logger = get_logger(__name__)
class DatabaseIter:
    def __init__(self, database_table_columns, **kwargs):
        self.database_table_columns=database_table_columns

        self.limit_per_table = kwargs.get('limit_per_table', None)

        self.config = ConfigHandler()
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(\
            self.config.connection['host'], self.config.connection['database'], \
            self.config.connection['user'], self.config.connection['password'])

        self.table_hash = self._get_indexable_schema_elements()
        self.stop_words = stopwords()
        
        self.tokenizer = re.compile("\\W+|_")
        self.url_match = re.compile("^(https?://)?\\w+\.[\\w+\./\~\?\&]+$")

    def _tokenize(self,text):
        
        if self.url_match.search(text) is not None:
            return ([text], True)
            
        tokenized = [word_part for word in text.lower().split() 
        for word_part in self.tokenizer.split(word.strip(string.punctuation))
        if word_part not in self.stop_words]
        double_tokenized = [ch_token for token in tokenized 
        for ch_token in self.tokenizer.split(token) 
        if ch_token not in self.stop_words]

        return ([token for token in double_tokenized if token != ''], False)

    def _schema_element_validator(self,table,column):
        return True

    def _get_indexable_schema_elements(self):
         with psycopg2.connect(self.conn_string) as conn:
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

            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cur:
                    '''
                    NOTE: Table and columns can't be directly passed as parameters.
                    Thus, the sql.SQL command with sql.Identifiers is used
                    '''

                    if self.limit_per_table is not None:
                        text = (f"SELECT ctid, {{}} FROM {{}} LIMIT {self.limit_per_table};")
                    else:
                        text = ("SELECT ctid, {} FROM {};")

                    cur.execute(
                        sql.SQL(text)
                        .format(sql.SQL(', ').join(sql.Identifier(col) for col in indexable_columns),
                                sql.Identifier(table))
                            )

                    arraysize = 100000
                    data = cur.fetchmany(arraysize)
                    while len(data) != 0:
                        for row in data:
                            ctid = row[0]
                            for col in range(1,len(row)):
                                column = cur.description[col][0]
                                (tokens, is_url) = self._tokenize( str(row[col]) )
                                for word in tokens:
                                    #print(tokens)
                                    if len([ch for ch in word if ch in string.punctuation]) != 0 and not is_url:
                                        print("Tokenizer not working {}".format(word, row[col]))
                                        sys.exit()
                                    yield table,ctid,column, word

                        data = cur.fetchmany(arraysize)
