from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import re
from statistics import mean

WORD_RE = re.compile(r"\w+")

# Напишите программу, которая находит среднюю длину слов

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            word_len = len(word)
            yield None, word_len        #записываем все длины в одну строку

    def reducer(self, length, length_list):
        yield 'Average', str(mean(length_list)) #находим среднее значение


if __name__ == '__main__':


    import time

    start_time = time.time()
    MRWordFreqCount.run()
    print("--- %s seconds ---" % (time.time() - start_time))