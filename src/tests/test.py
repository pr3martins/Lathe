import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

stop_words = stopwords.words("english")
queries = ["denzel washington","clint eastwood","john wayne","will smith","harrison ford","julia roberts","tom hanks","johnny depp","angelina jolie","morgan freeman","gone with the wind","star wars","casablanca","lord of the rings","the sound of music","wizard of oz","the notebook","forrest gump","the princess bride","the godfather","title atticus finch","title indiana jones","title james bond","title rick blaine","title will kane","title dr. hannibal lecter","title norman bates","title darth vader","title the wicked witch of the west","title nurse ratched","title frankly my dear i don't give a damn","title i'm going to make him an offer he can't refuse","title you don't understand i coulda had class i coulda been a contender i coulda been somebody instead of a bum which is what i am","title toto i've a feeling we're not in kansas any more","title here's looking at you kid","hamill skywalker","hanks 2004","henry fonda yours mine ours char_name","russell crowe gladiator char_name","brent spiner star trek","audrey hepburn 1951","name jacques clouseau","name jack ryan","rocky stallone","name terminator","harrison ford george lucas","sean connery fleming","reeves wachowski","dean jones herbie","indiana jones last crusade lost ark"]
for query in queries:
    word_tokens = re.split('[\s\']',query)
    keywords = [w for w in word_tokens if not w in stop_words]
    print(keywords)
