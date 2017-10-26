# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 11:27:43 2017

@author: andre
"""
import re

text = open("eulerGraphs.txt", "r").read()
text_split = text.split("\n\n") # split file (delimeter-empty row)
n=1

for graph in text_split: # for each split do
    graph_split0 = graph.replace('\n', ' ') # new line -> space
    graph_split1 = re.sub(r'^\d* ', '', graph_split0) # remove nodes
    graph_split = re.sub(r'^\d* ', '', graph_split1) # remove edges
    [open(str(n)+'.txt', 'w').write(graph_split)] # export results
    n += 1
    
#%%