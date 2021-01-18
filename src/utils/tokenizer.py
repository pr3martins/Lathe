import re
import string

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from utils import ConfigHandler
from utils import get_logger

logger = get_logger(__name__)
class Tokenizer:

    stop_words = set(stopwords.words('english'))

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
        if self.tokenize_method == 'nltk':
            word_tokens = word_tokenize(text)
            keywords = [w for w in word_tokens if not w in self.stop_words]
            return keywords

        if self.tokenize_method == 'simple':
            return [keyword.strip(string.punctuation)
                    for keyword in re.split('[\s\']', text.lower())
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
