import re
import string

from utils import ConfigHandler
from utils import get_logger

logger = get_logger(__name__)
class Tokenizer:

    stop_words = ('i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you',
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
                   "won't", 'wouldn', "wouldn't")


    def __init__(self, **kwargs):
        self.tokenize_method = kwargs.get('tokenize_method','simple')

        self.tokenizer = re.compile("\\W+|_")
        self.url_match = re.compile("^(https?://)?\\w+\.[\\w+\./\~\?\&]+$")
        self.config = ConfigHandler()


    def is_url(self,text):
        return self.url_match.search(text) is not None

    def compound_keywords(self,keyword_query):
        return [self.tokenize(segment) for segment in keyword_query.split('"')[1::2]]

    def keywords(self,keyword_query):
        return self.tokenize(keyword_query)

    def tokenize(self,text):
        if self.tokenize_method == 'simple':
            return [keyword.strip(string.punctuation)
                    for keyword in text.lower().split()
                    if keyword not in Tokenizer.stop_words]

        # TODO verificar referencial teorico
        # double tokenization
        tokenized = [word_part for word in text.lower().split()
        for word_part in self.tokenizer.split(word.strip(string.punctuation))
        if word_part not in self.stop_words]
        double_tokenized = [ch_token for token in tokenized
        for ch_token in self.tokenizer.split(token)
        if ch_token not in self.stop_words]

        return [token for token in double_tokenized if token != '']
