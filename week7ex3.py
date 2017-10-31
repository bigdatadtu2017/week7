# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 08:55:36 2017

@author: andre
"""

# import packages
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")
# MapReduce
class CommonName(MRJob):
    def mapper_get_names(self, _, line): # mapper that assigns value 1 to each key
        for name in WORD_RE.findall(line):
            yield (name, 1)       
    def combiner_count_names(self, name, counts): # sum values over grouped keys
        yield (name, sum(counts))       
    def reducer_count_names(self, name, counts): # send all keys and sumed values into one reducer
        yield None, (sum(counts), name)
    def reducer_find_most_common_name(self, _, counts): # find name with max accurances
        yield max(counts) 
    def steps(self): # connecting step outputs
        return [MRStep(mapper=self.mapper_get_names,
                   combiner=self.combiner_count_names,
                   reducer=self.reducer_count_names),
            MRStep(reducer=self.reducer_find_most_common_name)]
# final required component to execute MRJob
if __name__ == '__main__':
    CommonName.run()
