# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 12:47:42 2017

@author: andre
"""
# import packages
from mrjob.job import MRJob
from mrjob.step import MRStep

# MapReduce
class IsEulerTour(MRJob):
    
    def isEven(self, num): # Eulers Tour check function
        for node in num:
            if node % 2 != 0:
                return False
            return True; 
        
    def mapper_get_number(self, _, line): # mapper that assigns value 1 to each key
        nodes = line.split()
        for n in nodes:
            yield n, 1;
            
    def reducer_group_number(self, key, values): # sum values over grouped keys 
        yield None, sum(values);
        
    def reducer_euler_check(self, _, edges): # check if graph has Euler tour
        yield "Euler tour", self.isEven(edges);
        
    def steps(self): # connecting steps
        return [MRStep(mapper=self.mapper_get_number,
                   reducer=self.reducer_group_number),
            MRStep(reducer=self.reducer_euler_check)];  
# final required component to execute MRJob
if __name__ == '__main__':
    IsEulerTour.run()
