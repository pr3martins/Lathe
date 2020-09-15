import redis

from utils import ConfigHandler

class RedisHandler:
    def __init__(self):
        self.config = ConfigHandler()
        self.redis_connection = redis.Redis(host=self.config.redis['host'],\
            port=self.config_data.redis['port'], db=self.config.redis['db'],\
            charset='utf-8', decode_responses=True)
        self.attribute_collection = []
        
        
    def index_term(self, ctid, term, table, attribute):
        key = "{}.{}".format(table, attribute)
        
        if key not in self.attribute_collection:
            self.attribute_collection += [key]
            
        pipe = self.redis_connection = redis_connection.pipeline()
        clean_term = unidecode(term)
        attribute_idx = self.attribute_collection.index(key)
        pipe.zincrby("term:{}".format(clean_term), 1, attribute_idx)
        pipe.zincrby("attribute:{}".format(attribute_idx), 1, clean_term)
        pipe.rpush("ctid_list:{}:{}".format(attribute_idx,clean_term), ctid)
        pipe.incr("af:count:{}".format(clean_term))
    
        pipe.execute()
    
    def fit_index_data(self): 
        all_attributes = len(self.attribute_collection)
        pipe = self.redis_connection.pipeline()
        
        all_terms_weight_amount = [0.0] * (all_attributes)
        all_distinct_terms = [0] * (all_attributes)
        max_count_collection = [0] * (all_attributes)
        
        for attribute in self.redis_connection.scan_iter(match="attribute:*"):
            values = self.redis_connection.zrange(attribute, 0,  -1, withscores=True, desc=True)
            idx = int(attribute.split(':')[-1])
            max_frequency = values[0][1]
            distinct_terms = len(values)
            all_distinct_terms[idx] = distinct_terms
            max_count_collection[idx] = max_frequency

            pipe.delete(attribute)
            pipe.set('af:attr_max_frequency:{}'.format(attribute), max_frequency)
            pipe.set('iaf:attr_distinct_terms:{}'.format(attribute), distinct_terms)
        
        pipe.execute()
        
      
        pipe = self.redis_connection.pipeline()
        for term in self.redis_connection.scan_iter(match="term:*"): 
            values = self.redis_connection.zrange(term, 0,-1, withscores=True)
            term_occurrence = len(values)
            
            iaf = log(1 + (all_attributes/term_occurrence))
            pipe.set('iaf:value:{0}'.format(term), iaf)
            for (idx, count) in values:
                attribute = self.attribute_collection[int(idx)]
                attribute_distinct_terms = self.redis_connection.get('iaf:attr_distinct_terms:attribute:{0}'.format(idx))
                tf = log(1 + count) / log(1 + float(attribute_distinct_terms))
                all_terms_weight_amount[int(idx)] += (tf * iaf) * (tf * iaf)
                pipe.zadd("iaf:range:{0}".format(term), {idx: tf })
        
        for i,weight_term_amount in enumerate(all_terms_weight_amount):
            attribute_norm = sqrt(weight_term_amount)
            all_terms_weight_amount[i] = attribute_norm
            attribute = self.attribute_collection[i]
            pipe.set('iaf:norm:{}'.format(i), attribute_norm)
            pipe.set("idx:attribute:{}".format(i), attribute)
         
        pipe.execute()
                        
    
    
    def create_value_index(self):
        pass
    
    def create_schema_index(self):
        pass

        