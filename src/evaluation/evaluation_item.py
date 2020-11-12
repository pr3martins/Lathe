
from keyword_match import KeywordMatch

class EvaluationItem:
    
    def __init__(self):
        self.natural_language_query = ''
        self.sql_language_query = ''
        self.segments = []
        self.query_match = set()
        self.ranked_matches = []
        self.qm_position = -1
        self.id = -1 
        
    def build_from_json(self, json_data):
        self.natural_language_query = json_data['natural_language_query']
        self.sql_language_query = json_data['sql_query']
        self.segments = json_data['segments']
        self.id = json_data['id']
        
        for raw_kw_match in json_data['query_matches']:
            kw_match = KeywordMatch.from_json_serializable(raw_kw_match)
            self.query_match.add(kw_match)
                       
    def find_query_match_gt(self):
        for i, ranked_query_match in enumerate(self.ranked_matches):
            if ranked_query_match.matches == self.query_match:
                self.qm_position = i
                break
        
        print(self.qm_position, self.query_match, list(self.ranked_matches)[:5])
    
        
        
        