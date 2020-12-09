from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r'(?:[А-Яа-яA-za-z])[А-Яа-яA-Za-z]*\.')
e = 0.01

# Напишите программу, которая с помощью статистики определяет устойчивые сокращения вида пр., др., ...

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        #находим слова вида пр., др., ...
        for match in WORD_RE.findall(line):
            yield match.lower(), 1

    def combiner(self, word, count):
        yield None, (sum(count), word)      #объединяем все в одну строку, считаем количество повторений

    def reducer(self, _, pairs):
        sorted_pairs = sorted(pairs)        #сортируем по количеству повторений
        for count, word in sorted_pairs:
            percent = count / len(sorted_pairs)     #считаем долю встречаемости сокр слова
            if percent > e:
                yield word, str(percent)


if __name__ == '__main__':


    import time

    start_time = time.time()
    MRWordFreqCount.run()
    print("--- %s seconds ---" % (time.time() - start_time))