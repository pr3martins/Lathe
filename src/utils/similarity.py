import copy
import itertools
import numpy as np
from nltk.stem import WordNetLemmatizer
from utils import ConfigHandler
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn


class Similarity:
    def __init__(self, model, valueIndex ,schemaGraph):
        
        self.config = ConfigHandler()
        self.model = model
        self.attributeHash = attributeHash
        self.schemaGraph = schemaGraph
        
        self.loadEmbeddingHashes()
    
    
    def wordnet_similarity(self,wordA,wordB):
        A = set(wn.synsets(wordA))
        B = set(wn.synsets(wordB))

        wupSimilarities = [0]
        pathSimilarities = [0]
        
        for (sense1,sense2) in itertools.product(A,B):        
            wupSimilarities.append(wn.wup_similarity(sense1,sense2) or 0)
            pathSimilarities.append(wn.path_similarity(sense1,sense2) or 0)
            
        return max(max(wupSimilarities),max(pathSimilarities))

    def jaccard_similarity(self,wordA,wordB):

        A = set(wordA)
        B = set(wordB)

        return len(A & B) * 1. / len(A | B) * 1.
    
    
    def embedding10_similarity(self,word,table,column='*',Emb='B'):
        wnl = WordNetLemmatizer()
        
        sim_list = {}
        for word_item in self.EmbA[table].get(column, {}):
            sim_list[word_item] = self.EmbA[table][column][word_item]

        if column != '*':    
            if Emb == 'B':
                if column in self.EmbB[table].keys():
                    #sim_list |= self.EmbB[table][column]
                    for word_item in self.EmbB[table][column]:
                        if word_item in sim_list and \
                            self.EmbB[table][column][word_item] > sim_list[word_item]:
                            sim_list[word_item] = self.EmbB[table][column][word_item]
                        elif not word_item in sim_list:
                            sim_list[word_item] = self.EmbB[table][column][word_item]


            elif Emb == 'C':
                if column in self.EmbC[table].keys():
                    for word_item in self.EmbC[table][column]:
                        if word_item in sim_list and \
                            self.EmbC[table][column][word_item] > sim_list[word_item]:
                            sim_list[word_item] = self.EmbC[table][column][word_item]
                        elif not word_item in sim_list:
                            sim_list[word_item] = self.EmbC[table][column][word_item]

        sim = 0.0
        
        if word in sim_list or wnl.lemmatize(word) in sim_list:
            sim = max([sim_list.get(word, 0.0), sim_list.get(wnl.lemmatize(word), 0.0)])


        return sim
     
    
    def embedding_similarity(self,wordA,wordB):
        if wordA not in self.model or wordB not in self.model:
            return 0
        return self.model.similarity(wordA,wordB)
    
    
    def word_similarity(self,word,table,column = '*',
                    wn_sim=True, 
                    jaccard_sim=True,
                    emb_sim=True,
                    emb10_sim='B',
                    get_average=True):
        sim_list=[]

        if column == '*':
            schema_term = table
        else:
            schema_term = column

        if wn_sim:
            sim_list.append( self.wordnet_similarity(schema_term,word) )

        if jaccard_sim:
            sim_list.append( self.jaccard_similarity(schema_term,word) )

        if emb_sim:
            sim_list.append( self.embedding_similarity(schema_term,word) )

                
        if emb10_sim:
            sim_list.append(self.embedding10_similarity(word,table,column,emb10_sim))
        
        max_sim_list = sim_list[:]
        max_sim_list.sort(key=lambda x: x, reverse=True)
        
        sim = max_sim_list[0]
        
        if get_average:
            sim = (max_sim_list[0] + max_sim_list[1]) / 2.     
        
        return sim
    
    def __getSimilarSet(self,words, inputType = 'word'):
        return_list = {}
        sim_list = [None] * len(words)

        for i, word in enumerate(words):

            if inputType == 'vector':
                sim_list[i] = self.model.similar_by_vector(word)
            else:
                sim_list[i] = self.model.most_similar(word)


        for i, sim_list_item in enumerate(sim_list):
            if inputType == 'word':
                return_list.update({word.lower():sim \
                for word,sim in sim_list_item})
            else:
                return_list = {word.lower():sim \
                for word,sim in sim_list_item}

        return return_list

    def loadEmbeddingHashes(self,weight=0.5):
        
        self.EmbA = {}
        self.EmbB = {}
        self.EmbC = {}
        for table in self.attributeHash:
            is_case, indexes = is_camel_case(table)
            all_table_names = [table]
         
            if table not in self.model and is_case:
                indexes = [0] + indexes + [len(table)]
                all_table_names = [table[indexes[i]:indexes[i+1]].lower() for i in range(0,len(indexes)-1)]
        
            if table not in self.model and not is_case and '_' in table:
                all_table_names = table.split('_')
            
            self.EmbA[table]= {}
            self.EmbB[table]= {}
            self.EmbC[table]= {}
            
            self.EmbA[table]['*'] = self.__getSimilarSet(all_table_names) 

            for column in self.attributeHash[table]:    
                processed_column = column.split('_')
         
                
                word_vec = None 
                
                for i, item in enumerate(processed_column):
                    if item[-1].isdigit():
                        processed_column[i] = item[:-1]
             
                if len(processed_column) == 1:
                    is_case, column_indexes = is_camel_case(column)

                    if is_case:
                        all_column_names = [column]
                        column_indexes = [0] + column_indexes + [len(column)]
                        
                        processed_column = [column[column_indexes[i]:column_indexes[i+1]].lower() \
                        for i in range(0,len(column_indexes)-1)]
                        
                    processed_column = [c for c in processed_column if c in self.model]
                        
                           
                for simple_column in processed_column:
                    
                    if not simple_column in self.model:
                        continue

                    if word_vec is not None:
                        word_vec+=self.model[simple_column]
                    else:
                        word_vec=self.model[simple_column]*1.0

                    
                similar_set = {}
                similar_vector_set = {}  
                similar_numpy_set = {}
                avg_vec = None

                if len(processed_column) > 0:
                
                    similar_vector_set = self.__getSimilarSet(processed_column) #| similar_vector_set
                    similar_set = self.__getSimilarSet( [(t,c) for c in processed_column for t in all_table_names] ) #| similar_set
                    word_vec=self.model[simple_column]/len(processed_column)
                    sum_vec =None

                    for t in all_table_names:
                        if sum_vec is None:
                            sum_vec = copy.deepcopy(self.model[t])
                        else:
                            sum_vec = copy.deepcopy(self.model[t]) + sum_vec

                    avg_vec = ((sum_vec)*weight + word_vec*(1-weight))
                    similar_numpy_set = self.__getSimilarSet([avg_vec], inputType = 'vector')
                    

                self.EmbB[table][column]=similar_set
                self.EmbA[table][column]=similar_vector_set
                self.EmbC[table][column]=similar_numpy_set

        
       
        G = self.schemaGraph
        for tableA in G.tables():

            if tableA not in self.attributeHash or tableA not in self.model:
                continue

            for tableB in G.getAdjacentTables(tableA):

                if tableB not in self.attributeHash or tableB not in self.model:
                    continue

                self.EmbB[tableB][tableA] = self.EmbB[tableA][tableB] = self.__getSimilarSet( (tableA,tableB) )    

