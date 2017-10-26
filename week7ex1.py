# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 08:55:36 2017

@author: andre
"""

# import packages
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# leave only words in a text file
WORD_RE = re.compile(r"[\w']+")

# MapReduce
class MRMostUsedWord(MRJob):
    # mapper that assigns value 1 to each key
    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
    # sum values over grouped keys 
    def reducer_count_words(self, word, counts):
        yield (word, sum(counts))
        
    # connecting step outputs    
    def steps(self):
        return [MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words)]

# final required component to execute MRJob
if __name__ == '__main__':
    MRMostUsedWord.run()

