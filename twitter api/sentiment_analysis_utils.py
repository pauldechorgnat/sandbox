from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param

from nltk.stem import SnowballStemmer
import re


class CustomTokenizer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, stopwords=None, pseudo=None, link=None):
        super(CustomTokenizer, self).__init__()

        self.stopwords = Param(self, "stopwords", "")
        self._setDefault(stopwords=set())

        self.link = Param(self, "link", "")
        self._setDefault(link="")

        self.pseudo = Param(self, "pseudo", "")
        self._setDefault(pseudo="")

        self.stemmer = Param(self, "stemmer", "")
        self._setDefault(stemmer=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None, pseudo=None, link=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setStopwords(self, value):
        self._paramMap[self.stopwords] = value
        return self

    def getStopwords(self):
        return self.getOrDefault(self.stopwords)

    def setLink(self, value):
        self._paramMap[self.link] = value
        return self

    def getLink(self):
        return self.getOrDefault(self.link)

    def setPseudo(self, value):
        self._paramMap[self.pseudo] = value
        return self

    def getPseudo(self):
        return self.pseudo

    def _transform(self, dataset):
        stopwords = self.getStopwords()
        link_re = self.getLink()
        pseudo_re = self.getPseudo()
        word_re = re.compile('\\w+')
        stemmer = SnowballStemmer(language='english')

        def f(s):

            text = re.sub(pattern='RT:', string=s, repl='rttwittersubstitute')
            text = re.sub(pattern=link_re, string=text, repl='pseudotwittersubstitute')
            text = re.sub(pattern=pseudo_re, string=text, repl='linktwittersubstitute')

            tokens = [t for t in word_re.findall(string=text.lower()) if t not in stopwords]
            tokens = [stemmer.stem(t) for t in tokens if stemmer.stem(t) not in stopwords]
            return ' '.join(tokens)


# deprecated
def pre_processing_tweets(
        dictionary,
        stop_words,
        stemMer_,
        word_=re.compile('\\w+'),
        address_=re.compile('\\@[A-Za-z0-9]*]'),
        link_=re.compile('http(s)?://[a-zA-Z0-9./\\-]*')
):
    text = dictionary['text']
    target = dictionary['polarity']
    rt = ('RT' in text) * 1
    addressed = ('@' in text) * 1
    link = ('http' in text) * 1
    text = re.sub(pattern=address_, string=text, repl='')
    text = re.sub(pattern=link_, string=text, repl='')
    tokens = [t for t in word_.findall(text) if t not in stop_words]
    tokens = [stemMer_.stem(t) for t in tokens if stemMer_.stem(t) not in stop_words]

    new_dictionary = {
        'target': target,
        'tokens': tokens,
        'RT': rt,
        'addressed': addressed,
        'link': link
    }
    return new_dictionary


if __name__ == "__main__":
    CustomTokenizer()
