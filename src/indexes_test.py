from index.value_index import *
from pprint import pprint as pp
filename = 'assets/persistent_indexes/value_index.shelve'

x=ValueIndex()
x.add_mapping('word','table','attribute','ctid')
x.add_mapping('paulo','person','name','1997')
x.add_mapping('brandell','person','name','1996')
x.persist_to_shelve(filename)
print('Babel')
print(BabelHash.babel)

print('\nValue Index X criado:')
pp(x)
print('\n',x)

BabelHash.babel = {}
y = ValueIndex.load_from_shelve(filename, keywords = ['paulo','brandell','word'])

print('\nNovo Babel')
print(BabelHash.babel)

print("\nValue Index Y carregando apenas as keywors 'paulo' e 'brandell'")
pp(y)
print('\n',y)


print('\nApesar de que a tabela de tradução do BabelHash possa mudar, a consistência dos dados é garantida.')
