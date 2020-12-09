from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")

# Все слова, которые более чем в половине случаев начинаются с большой буквы и встречаются больше 10 раз

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            is_upper = word[0].isupper()    #определяем, начинается ли с большой буквы
            yield word.lower(), is_upper    #key - слово, value - true/false

    def reducer(self, word, is_upper):
        upper_count = 0     #количество слов с большой буквы
        number_count = 0    #общее количество слов
        for upper in is_upper:
            if upper:
                upper_count += 1
            number_count += 1

        case_count = number_count/2     #считаем среднее количество случаев
        if number_count > 10 and upper_count > case_count:      #главное условие задачи
            yield str(number_count), word           #записываем в текущий Part количество повторений и слово



if __name__ == '__main__':


    import time

    start_time = time.time()
    MRWordFreqCount.run()
    print("--- %s seconds ---" % (time.time() - start_time))