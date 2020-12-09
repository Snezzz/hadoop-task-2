from mrjob.job import MRJob
import pymorphy2
from mrjob.protocol import TextProtocol
import re


# Напишите программу, которая с помощью статистики находит имена, употребляющиеся в статьях

WORD_RE = re.compile(r"[\w']+")
e = 0.8
morph = pymorphy2.MorphAnalyzer()


class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol


    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word, 1

    def combiner(self, word, counts):
        yield None, word        #объединяем в одну строку

    def reducer(self, _, words):
        for word in words:
            for result in morph.parse(word):        #разбираем слово
                tag = result.tag            # набор граммем, характеризующих данное слово
                score = result.score        # оценка вероятности того, что данный разбор правильный
                if 'Name' in tag and score >= e:        #нашли имя и имеем правильный разбор (P(tag|word) = 80%)
                    yield word, str(score)


if __name__ == '__main__':


    import time

    start_time = time.time()
    MRWordFreqCount.run()
    print("--- %s seconds ---" % (time.time() - start_time))