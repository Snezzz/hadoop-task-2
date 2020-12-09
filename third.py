from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"[a-zA-Z']+")

# Напишите программу, которая находит самое частоупотребляемое слово, состоящее из латинских букв

class MRWordFreqCount(MRJob):


    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1   #key - слово в нижнем регистре, value = 1

    def combiner(self, word, count):
        yield word, sum(count)      #объединяем по ключу и считаем количество одинаковых слов

    def reducer_words_count(self, word, counts):
        yield None, (sum(counts), word)     #записываем все в одну строку со значением общей суммы слова и самого слова

    def reducer_find_max(self, _, count_word):
        yield max(count_word)       #находим самое частоупотребляемое слово

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_words_count),
            MRStep(reducer=self.reducer_find_max)
        ]


if __name__ == '__main__':


    import time

    start_time = time.time()
    MRWordFreqCount.run()
    print("--- %s seconds ---" % (time.time() - start_time))