from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")

# Напишите программу, которая находит самое длинное слово

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            word_len = len(word)
            yield str(word_len), word  #key - длина слова, value - слово

    def help_reducer(self, length, words):
        yield None, (",".join(words), length)  #объединяем для поиска макс значения

    def reducer_find_max_length_word(self, _, word_len_pairs):
        yield max(word_len_pairs)   #ищем макс значение

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.help_reducer),
            MRStep(reducer=self.reducer_find_max_length_word)
        ]


if __name__ == '__main__':


    import time

    start_time = time.time()
    MRWordFreqCount.run()
    print("--- %s seconds ---" % (time.time() - start_time))