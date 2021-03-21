# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.9.1
#   kernelspec:
#     display_name: k2d
#     language: python
#     name: k2d
# ---

x = [('casting', '__search_id'),
 ('casting', 'id'),
 ('casting', 'movie_id'),
 ('casting', 'note'),
 ('casting', 'nr_order'),
 ('casting', 'person_id'),
 ('casting', 'person_role_id'),
 ('casting', 'role_id'),
 ('character', '__search_id'),
 ('character', 'id'),
 ('character', 'imdb_id'),
 ('character', 'imdb_index'),
 ('character', 'name'),
 ('character', 'name_pcode_nf'),
 ('character', 'surname_pcode'),
 ('movie', '__search_id'),
 ('movie', 'episode_nr'),
 ('movie', 'episode_of_id'),
 ('movie', 'id'),
 ('movie', 'imdb_id'),
 ('movie', 'imdb_index'),
 ('movie', 'kind_id'),
 ('movie', 'phonetic_code'),
 ('movie', 'season_nr'),
 ('movie', 'series_years'),
 ('movie', 'title'),
 ('movie', 'year'),
 ('movieinfo', '__search_id'),
 ('movieinfo', 'id'),
 ('movieinfo', 'info'),
 ('movieinfo', 'info_type_id'),
 ('movieinfo', 'movie_id'),
 ('movieinfo', 'note'),
 ('person', '__search_id'),
 ('person', 'id'),
 ('person', 'imdb_id'),
 ('person', 'imdb_index'),
 ('person', 'name'),
 ('person', 'name_pcode_cf'),
 ('person', 'name_pcode_nf'),
 ('person', 'surname_pcode'),
 ('role', '__search_id'),
 ('role', 'id'),
 ('role', 'type')]

# +
results = {}
for table,attribute in x:
    results.setdefault(table,[]).append(attribute)
    
json_results= []
for table in results:
    item = {}
    item['table']=table
    item['attributes']=results[table]
    json_results.append(item)


import json
print(json.dumps(json_results,indent=4))
# -

x= 8.580612566301306e-15

x

f'{x:.3e}'



# +
import sys
sys.path.append('../')
from candidate_network import CandidateNetwork

complete = [{'keyword_match': {'table': 'movie', 'schema_filter': [], 'value_filter': [{'attribute': 'title', 'keywords': ['jones', 'indiana', 'last', 'crusade']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'casting', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'person', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'casting', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t3', 't5'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'movie', 'schema_filter': [], 'value_filter': [{'attribute': 'title', 'keywords': ['ark', 'lost']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}]
cn = CandidateNetwork.from_json_serializable(complete)
cn

# +
print(cn)
print('''MOVIE.v(title{jones,indiana,crusade,last})
	<CASTING
        >MOVIE.v(title{lost,ark})
		>PERSON
			<CASTING''')


# -


# MOVIE.v(title{jones,indiana,crusade,last})
# 	<CASTING
# 		>PERSON
# 			<CASTING
# 				>MOVIE.v(title{lost,ark})
# ```
# ```

MOVIE.v(title{lost,ark})
	<CASTING
		>PERSON(a)
			<CASTING
                >MOVIE.v(title{jones,indiana,crusade,last})9

# +
>PERSON(a)
    <CASTING
        >MOVIE.v(title{jones,indiana,crusade,last})
    <CASTING
        >MOVIE.v(title{lost,ark})
        
new_node
# -

0  Pa(CASTING:2)
1  CASTING(movie(indiana):1) CASTING(movie(lost, ark):1)
2  movie() movie()

from queue import deque
from collections import Counter

graph = {
    (1,'A'):[(2,'B')],
    (2,'B'):[(1,'A'),(3,'C')],
    (3,'C'):[(2,'B'),(4,'B')],
    (4,'B'):[(3,'C'),(5,'D')],
    (5,'D'):[(4,'B')],
}

graph = {
    (1,'A'):[(2,'C'),(3,'C')],
    (2,'C'):[(1,'A'),(4,'B')],
    (3,'C'):[(1,'A'),(5,'E')],
    (4,'B'):[(2,'C')],
    (5,'E'):[(3,'C')],
}

# +
level = 0
store = []

visited = set()
root = (1,'A')

queue = deque()
queue.append( (level,root) )

while queue:
    level,vertex = queue.popleft()
    alias,label = vertex
    sons = Counter()
    
    visited.add(vertex)
    
    for adj_vertex in graph[vertex]:
        if adj_vertex in visited:
            print(vertex,adj_vertex)
            continue
        adj_alias,adj_label = adj_vertex
        
        queue.append( (level+1,adj_vertex) )
        sons[adj_label]+=1
        
    if len(store)<level+1:
        store.append(set())
    
    store[level].add( (label,frozenset(sons.items())) )
tuple(frozenset(items) for items in store)
# -

a=Counter(A=2,E=1,F=1)
b=Counter(B=1)
a-b, b-a

a=Counter(A=2,E=1,F=1)
b=Counter(E=1,F=1)
sum((a-b).values())

Counter(A=1)-Counter(B=1)

from collections import OrderedDict

x = dict([('Z',1),('a',2)])

list(reversed(graph.keys()))

(2,'B'),(1,'A')

(2,'B'),('A')

x= frozenset(Counter(x for x in ['A','A','B','B','B']).items())
x

Counter((a,b) for a,b in x)

Counter(dict(x)) - dict(x)

x = Counter()

x[2]+=1

set([1]).remove(1)

raise ValueError('The root of a Candidate Network cannot be a Keyword-Free Match.')

# +
from string import punctuation,ascii_uppercase,ascii_lowercase
translate_table = str.maketrans(ascii_uppercase+punctuation,ascii_lowercase+' '*len(punctuation))



s.translate(translate_table)
# -

s= 'Paulo'
s.translate(str.maketrans('Po', 'bu', string.punctuation))

# +
string.punctuation

upper_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
lower_letters = 'abcdefghijklmnopqrstuvwxyz'
# -

'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

# +
# str.maketrans(string.punctuation,' '*len(string.punctuation))
# -

str.maketrans(ascii_uppercase+punctuation,ascii_lowercase+' '*len(punctuation))


import re
import string

# +
text = '''
[Tom hands Max an envelope]::Max California: What is this?::Tom Welles: Money. People use it to buy goods and services.
 [to porn store customer]::Max California: Hey! It's like a gas station, you pay before you pump!
 Max California: There are some things that you see, and you can't unsee them. Know what I mean?
 Daniel Longdale: Do you think people like the Christians hire us to invite us to their dinner parties? It's our job to clean up their royal messes.
 Dino Velvet: You trust me to take your money, but not your picture?::Tom Welles: Those are two different kinds of trust.
 Max California: Can I interest you in a battery-operated vagina?::Tom Welles: No thank you.::Max California: Are you sure? I'd hate for you to be in one of those everyda
y situations that calls for a battery-operated vagina and not have one.
 Tom Welles: What're you reading?::[Max California shows book - "Anal Secretary"]::Tom Welles: Catchy title. What are you really reading? Hard to believe that book's got 
any parts worth highlighting.::[Max California reveals real book - "In Cold Blood"]::Tom Welles: Capote!::Max California: Yeah, well. You know how it is.::Tom Welles: Yea
h. Wouldn't want to embarrass yourself in front of your fellow perverts.::Max California: That's right. I might get drummed out of the Pornographer's Union. Where would I be then?
'''
re_punctuation = re.compile('[^\w\d]+')
re_spaces = re.compile('\s+')


new = re.sub(re_punctuation,' ', text.lower())
new = re.split(re_spaces, new)
new


# -

def test(x="teste"):
    return x


from copy import deepcopy,copy
x=[1,2,3]
y = copy(x)
x.append(4)
x,y

x=2

x=2
x**=3
x

x = [
    {
        "table": "borders",
        "attributes": [
            "__search_id",
            "country1",
            "country2",
            "length"
        ]
    },
    {
        "table": "city",
        "attributes": [
            "__search_id",
            "country",
            "latitude",
            "longitude",
            "name",
            "population",
            "province"
        ]
    },
    {
        "table": "continent",
        "attributes": [
            "__search_id",
            "area",
            "name"
        ]
    },
    {
        "table": "country",
        "attributes": [
            "__search_id",
            "area",
            "capital",
            "code",
            "name",
            "population",
            "province"
        ]
    },
    {
        "table": "desert",
        "attributes": [
            "__search_id",
            "area",
            "name"
        ]
    },
    {
        "table": "economy",
        "attributes": [
            "__search_id",
            "agriculture",
            "country",
            "gdp",
            "industry",
            "inflation",
            "service"
        ]
    },
    {
        "table": "encompasses",
        "attributes": [
            "__search_id",
            "continent",
            "country",
            "percentage"
        ]
    },
    {
        "table": "ethnic_group",
        "attributes": [
            "__search_id",
            "country",
            "name",
            "percentage"
        ]
    },
    {
        "table": "geo_desert",
        "attributes": [
            "__search_id",
            "country",
            "desert",
            "province"
        ]
    },
    {
        "table": "geo_island",
        "attributes": [
            "__search_id",
            "country",
            "island",
            "province"
        ]
    },
    {
        "table": "geo_lake",
        "attributes": [
            "__search_id",
            "country",
            "lake",
            "province"
        ]
    },
    {
        "table": "geo_mountain",
        "attributes": [
            "__search_id",
            "country",
            "mountain",
            "province"
        ]
    },
    {
        "table": "geo_river",
        "attributes": [
            "__search_id",
            "country",
            "province",
            "river"
        ]
    },
    {
        "table": "geo_sea",
        "attributes": [
            "__search_id",
            "country",
            "province",
            "sea"
        ]
    },
    {
        "table": "is_member",
        "attributes": [
            "__search_id",
            "country",
            "organization",
            "type"
        ]
    },
    {
        "table": "island",
        "attributes": [
            "__search_id",
            "area",
            "coordinates",
            "islands",
            "name"
        ]
    },
    {
        "table": "lake",
        "attributes": [
            "__search_id",
            "area",
            "name"
        ]
    },
    {
        "table": "language",
        "attributes": [
            "__search_id",
            "country",
            "name",
            "percentage"
        ]
    },
    {
        "table": "located",
        "attributes": [
            "__search_id",
            "city",
            "country",
            "lake",
            "province",
            "river",
            "sea"
        ]
    },
    {
        "table": "merges_with",
        "attributes": [
            "__search_id",
            "sea1",
            "sea2"
        ]
    },
    {
        "table": "mountain",
        "attributes": [
            "__search_id",
            "coordinates",
            "height",
            "name"
        ]
    },
    {
        "table": "organization",
        "attributes": [
            "__search_id",
            "abbreviation",
            "city",
            "country",
            "established",
            "name",
            "province"
        ]
    },
    {
        "table": "politics",
        "attributes": [
            "__search_id",
            "country",
            "government",
            "independence"
        ]
    },
    {
        "table": "population",
        "attributes": [
            "__search_id",
            "country",
            "infant_mortality",
            "population_growth"
        ]
    },
    {
        "table": "province",
        "attributes": [
            "__search_id",
            "area",
            "capital",
            "capprov",
            "country",
            "name",
            "population"
        ]
    },
    {
        "table": "religion",
        "attributes": [
            "__search_id",
            "country",
            "name",
            "percentage"
        ]
    },
    {
        "table": "river",
        "attributes": [
            "__search_id",
            "lake",
            "length",
            "name",
            "river",
            "sea"
        ]
    },
    {
        "table": "sea",
        "attributes": [
            "__search_id",
            "depth",
            "name"
        ]
    }
]



# +
import psycopg2

conn_string = f"host='localhost' dbname='mondial_coffman' user='paulo' password=''"

with psycopg2.connect(conn_string) as conn:
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
            if column == '__search_id':
                continue
            table_hash.setdefault(table,[]).append(column)
# -

table_hash

# +
import json
json_text = [
    {'table':table,'attributes':attributes}
    for table,attributes in table_hash.items()
]

print(json.dumps(json_text,indent=4))
# -

x=[{'keyword_query': 'thailand', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['thailand']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['thailand']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'netherlands', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['netherlands']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['netherlands']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'georgia', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['georgia']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['georgia']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'country china', 'query_matches': [[{'table': 'country', 'schema_filter': [{'attribute': '*', 'keywords': ['country']}], 'value_filter': [{'attribute': 'name', 'keywords': ['china']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [{'attribute': '*', 'keywords': ['country']}], 'value_filter': [{'attribute': 'name', 'keywords': ['china']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'bangladesh', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['bangladesh']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['bangladesh']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'alexandria', 'query_matches': [[{'table': 'city', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['alexandria']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'city', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['alexandria']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'sonsonate', 'query_matches': [[{'table': 'city', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['sonsonate']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'city', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['sonsonate']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'xiaogan', 'query_matches': [[{'table': 'city', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['xiaogan']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'city', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['xiaogan']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'city glendale', 'query_matches': [[{'table': 'city', 'schema_filter': [{'attribute': '*', 'keywords': ['city']}], 'value_filter': [{'attribute': 'name', 'keywords': ['glendale']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'city', 'schema_filter': [{'attribute': '*', 'keywords': ['city']}], 'value_filter': [{'attribute': 'name', 'keywords': ['glendale']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'city granada', 'query_matches': [[{'table': 'city', 'schema_filter': [{'attribute': '*', 'keywords': ['city']}], 'value_filter': [{'attribute': 'name', 'keywords': ['granada']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'city', 'schema_filter': [{'attribute': '*', 'keywords': ['city']}], 'value_filter': [{'attribute': 'name', 'keywords': ['granada']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'Lake Kariba', 'query_matches': [[{'table': 'lake', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['lake', 'kariba']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'lake', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['lake', 'kariba']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'Niger', 'query_matches': [[{'table': 'river', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['niger']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'river', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['niger']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'Arabian Sea', 'query_matches': [[{'table': 'sea', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['arabian', 'sea']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'sea', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['arabian', 'sea']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'Asauad', 'query_matches': [[{'table': 'desert', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['asauad']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'desert', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['asauad']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'Sardegna', 'query_matches': [[{'table': 'island', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['sardegna']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'island', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['sardegna']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'arab cooperation council', 'query_matches': [[{'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['arab', 'cooperation', 'council']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['arab', 'cooperation', 'council']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'world labor', 'query_matches': [[{'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['labor', 'world']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['labor', 'world']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'islamic conference', 'query_matches': [[{'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['conference', 'islamic']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['conference', 'islamic']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': '30 group', 'query_matches': [[{'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['group', '30']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['group', '30']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'caribbean economic', 'query_matches': [[{'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['caribbean', 'economic']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['caribbean', 'economic']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': []}]]}, {'keyword_query': 'cameroon economy', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['cameroon']}]}, {'table': 'economy', 'schema_filter': [{'attribute': '*', 'keywords': ['economy']}], 'value_filter': []}]], 'candidate_networks': [[{'keyword_match': {'table': 'economy', 'schema_filter': [{'attribute': '*', 'keywords': ['economy']}], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['cameroon']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'nigeria gdp', 'query_matches': [[{'table': 'economy', 'schema_filter': [{'attribute': 'gdp', 'keywords': ['gdp']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['nigeria']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'economy', 'schema_filter': [{'attribute': 'gdp', 'keywords': ['gdp']}], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['nigeria']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'mongolia republic', 'query_matches': [[{'table': 'politics', 'schema_filter': [], 'value_filter': [{'attribute': 'government', 'keywords': ['republic']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['mongolia']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['mongolia']}]}, 'alias': 't2', 'outgoing_neighbours': [], 'incoming_neighbours': ['t1']}, {'keyword_match': {'table': 'politics', 'schema_filter': [], 'value_filter': [{'attribute': 'government', 'keywords': ['republic']}]}, 'alias': 't1', 'outgoing_neighbours': ['t2'], 'incoming_neighbours': []}]]}, {'keyword_query': 'kiribati politics', 'query_matches': [[{'table': 'politics', 'schema_filter': [{'attribute': '*', 'keywords': ['politics']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['kiribati']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'politics', 'schema_filter': [{'attribute': '*', 'keywords': ['politics']}], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['kiribati']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'poland language', 'query_matches': [[{'table': 'language', 'schema_filter': [{'attribute': '*', 'keywords': ['language']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['poland']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['poland']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'language', 'schema_filter': [{'attribute': '*', 'keywords': ['language']}], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}]]}, {'keyword_query': 'spain galician', 'query_matches': [[{'table': 'language', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['galician']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['spain']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'language', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['galician']}]}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['spain']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'uzbekistan eastern orthodox', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['uzbekistan']}]}, {'table': 'religion', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['eastern', 'orthodox']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['uzbekistan']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'religion', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['eastern', 'orthodox']}]}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}]]}, {'keyword_query': 'haiti religion', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['haiti']}]}, {'table': 'religion', 'schema_filter': [{'attribute': '*', 'keywords': ['religion']}], 'value_filter': []}]], 'candidate_networks': [[{'keyword_match': {'table': 'religion', 'schema_filter': [{'attribute': '*', 'keywords': ['religion']}], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['haiti']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'suriname ethnic_group', 'query_matches': [[{'table': 'ethnic_group', 'schema_filter': [{'attribute': '*', 'keywords': ['ethnic_group']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['suriname']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'ethnic_group', 'schema_filter': [{'attribute': '*', 'keywords': ['ethnic_group']}], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['suriname']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'slovakia german', 'query_matches': [[{'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['german']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['slovakia']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['slovakia']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['german']}]}, 'alias': 't2', 'outgoing_neighbours': ['t1'], 'incoming_neighbours': []}]]}, {'keyword_query': 'poland cape verde organization', 'query_matches': [[{'table': 'organization', 'schema_filter': [{'attribute': '*', 'keywords': ['organization']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['verde', 'cape']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['poland']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [{'attribute': '*', 'keywords': ['organization']}], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['poland']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['verde', 'cape']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t3', 't5'], 'incoming_neighbours': []}]]}, {'keyword_query': 'saint kitts cambodia', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['saint', 'kitts']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['cambodia']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['saint', 'kitts']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['cambodia']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t5', 't3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'marshall islands grenadines organization', 'query_matches': [[{'table': 'organization', 'schema_filter': [{'attribute': '*', 'keywords': ['organization']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['grenadines']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['marshall', 'islands']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['marshall', 'islands']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'organization', 'schema_filter': [{'attribute': '*', 'keywords': ['organization']}], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['grenadines']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t3', 't5'], 'incoming_neighbours': []}]]}, {'keyword_query': 'czech republic cote divoire organization', 'query_matches': [[{'table': 'organization', 'schema_filter': [{'attribute': '*', 'keywords': ['organization']}], 'value_filter': []}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['divoire', 'cote']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['czech', 'republic']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['divoire', 'cote']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['czech', 'republic']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'organization', 'schema_filter': [{'attribute': '*', 'keywords': ['organization']}], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t3', 't5'], 'incoming_neighbours': []}]]}, {'keyword_query': 'panama oman', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['oman']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['panama']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['oman']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['panama']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t3', 't5'], 'incoming_neighbours': []}]]}, {'keyword_query': 'iceland mali', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['mali']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['iceland']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['iceland']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['mali']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t5', 't3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'guyana sierra leone', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['sierra', 'leone']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['guyana']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['sierra', 'leone']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['guyana']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t3', 't5'], 'incoming_neighbours': []}]]}, {'keyword_query': 'mauritius india', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['india']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['mauritius']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['mauritius']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['india']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t5', 't3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'vanuatu afghanistan', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['afghanistan']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['vanuatu']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['vanuatu']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['afghanistan']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t5', 't3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'libya australia', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['australia']}]}, {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['libya']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'organization', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['australia']}]}, 'alias': 't5', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['libya']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'is_member', 'schema_filter': [], 'value_filter': []}, 'alias': 't4', 'outgoing_neighbours': ['t5', 't3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'hutu africa', 'query_matches': [[{'table': 'continent', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['africa']}]}, {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['hutu']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'continent', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['africa']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'encompasses', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['hutu']}]}, 'alias': 't4', 'outgoing_neighbours': ['t3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2', 't4']}]]}, {'keyword_query': 'serb europe', 'query_matches': [[{'table': 'continent', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['europe']}]}, {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['serb']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'continent', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['europe']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'encompasses', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['serb']}]}, 'alias': 't4', 'outgoing_neighbours': ['t3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'uzbek asia', 'query_matches': [[{'table': 'continent', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['asia']}]}, {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['uzbek']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'continent', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['asia']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'encompasses', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': [], 'incoming_neighbours': ['t4', 't2']}, {'keyword_match': {'table': 'ethnic_group', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['uzbek']}]}, 'alias': 't4', 'outgoing_neighbours': ['t3'], 'incoming_neighbours': []}]]}, {'keyword_query': 'rhein germany', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['germany']}]}, {'table': 'river', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['rhein']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'river', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['rhein']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'geo_river', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t1', 't3'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['germany']}]}, 'alias': 't4', 'outgoing_neighbours': [], 'incoming_neighbours': ['t3']}, {'keyword_match': {'table': 'province', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': ['t4'], 'incoming_neighbours': ['t2']}]]}, {'keyword_query': 'egypt nile', 'query_matches': [[{'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['egypt']}]}, {'table': 'river', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['nile']}]}]], 'candidate_networks': [[{'keyword_match': {'table': 'geo_river', 'schema_filter': [], 'value_filter': []}, 'alias': 't2', 'outgoing_neighbours': ['t3', 't1'], 'incoming_neighbours': []}, {'keyword_match': {'table': 'river', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['nile']}]}, 'alias': 't1', 'outgoing_neighbours': [], 'incoming_neighbours': ['t2']}, {'keyword_match': {'table': 'country', 'schema_filter': [], 'value_filter': [{'attribute': 'name', 'keywords': ['egypt']}]}, 'alias': 't4', 'outgoing_neighbours': [], 'incoming_neighbours': ['t3']}, {'keyword_match': {'table': 'province', 'schema_filter': [], 'value_filter': []}, 'alias': 't3', 'outgoing_neighbours': ['t4'], 'incoming_neighbours': ['t2']}]]}]

# +
{
    km['table']: [
        mapping['attribute']
        for mapping in km['schema_filter']+km['value_filter']
    ]
    for item in x
    for qm in item['query_matches']
    for km in qm
    
}
# -

import itertools
d = [(1,2),(1,3)]
{
    table:[attribute for table,attribute in group]
    for table,group in itertools.groupby(d,lambda x:x[0])
}
