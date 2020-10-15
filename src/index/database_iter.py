import re
import psycopg2
from psycopg2 import sql
import string
from utils import ConfigHandler, get_logger

logger = get_logger(__name__)
class DatabaseIter:
    def __init__(self, showLog=True):
        self.config = ConfigHandler()

        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(\
            self.config.connection['host'], self.config.connection['database'], \
            self.config.connection['user'], self.config.connection['password'])

        self.table_hash = self._get_indexable_schema_elements()
        self.showLog = showLog
        self.stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you',
                           "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself',
                           'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her',
                           'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them',
                           'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom',
                           'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are',
                           'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
                           'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and',
                           'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at',
                           'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through',
                           'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up',
                           'down','in', 'out', 'on', 'off', 'over', 'under', 'again', 'further',
                           'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all',
                           'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such',
                           'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
                           's', 't', 'can', 'just', 'don', "don't", 'should', "should've", 'now',
                           'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
                           "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't",
                           'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn',
                           "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't",
                           'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won',
                           "won't", 'wouldn', "wouldn't"]
        self.tokenizer = re.compile("\\W+")
        self.url_match = re.compile(("^(https?://)?\\w+\.[\\w+\./\~\?\&]+$")

    def _tokenize(self,text):
        if self.url_match.search(text) is not None:
            return [text]
            
        tokenized = [self.tokenizer.split(word.strip(string.punctuation))
                for word in text.lower().split()
                if word not in self.stop_words]
        return [token for token in tokenized if token != '']

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
                        AND tc.constraint_name IS NULL
                '''
                cur.execute(GET_TABLE_AND_COLUMNS_WITHOUT_FOREIGN_KEYS_SQL)
                table_hash = {}
                for table,column in cur.fetchall():
                    if table in self.config.remove_from_index:
                        continue

                    if column == '__search_id':
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
                    cur.execute(
                        sql.SQL("SELECT ctid, {} FROM {};")
                        .format(sql.SQL(', ').join(sql.Identifier(col) for col in indexable_columns),
                                sql.Identifier(table))
                            )
                    data = cur.fetchmany(1000)
                    while len(data) != 0:
                        for row in data:
                            ctid = row[0]
                            for col in range(1,len(row)):
                                column = cur.description[col][0]
                                for word in self._tokenize( str(row[col]) ):
                                    yield table,ctid,column, word
                        data = cur.fetchmany(1000)
