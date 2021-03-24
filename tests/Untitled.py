# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.9.1
#   kernelspec:
#     display_name: k2db
#     language: python
#     name: k2db
# ---

from k2db.utils.config_handler import ConfigHandler
import psycopg2
from psycopg2.sql import SQL, Identifier
from pprint import pprint as pp

config = ConfigHandler()

config.connection['database']='mondial_coffman'

with psycopg2.connect(**config.connection) as conn:
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

        fk_constraints = {}

        for row in cur.fetchall():
            constraint,table,attribute,foreign_table,foreign_attribute = (column.strip('\"') for column in row)
            fk_constraints.setdefault((constraint,table),(table,foreign_table,[]))[2].append( (attribute,foreign_attribute) )

with psycopg2.connect(**config.connection) as conn:
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

        fk_constraints = {}

        for row in cur.fetchall():
            constraint,table,attribute,foreign_table,foreign_attribute = (column.strip('\"') for column in row)
            fk_constraints.setdefault((constraint,table),(table,foreign_table,[]))[2].append( (attribute,foreign_attribute) )
            
        
        for constraint in fk_constraints:
            table,foreign_table,attribute_mappings = fk_constraints[constraint]
            sql = '''
            SELECT NOT EXISTS (
                SELECT COUNT(t1.*), {}
                FROM {} t1
                GROUP BY {}
                HAVING COUNT(t1.*)>1
            )
            '''

            attributes_param = SQL(', ').join(
                Identifier(attribute) 
                for attribute,foreign_attribute in attribute_mappings
            )

            smt= SQL(sql).format(
                attributes_param,
                Identifier(table),
                attributes_param,
            )
            cur.execute(smt)
            
            cardinality = '1:1' if cur.fetchone()[0] else 'N:1'            
            fk_constraints[constraint] = (cardinality,table,foreign_table,attribute_mappings)

pp(fk_constraints)


def f():
    for i in range(3):
        yield i


x = f()

import pickle

pickle.dumps({1:2})

import json

json.dumps({1:2})
