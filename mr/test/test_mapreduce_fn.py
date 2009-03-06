# test_mapreduce_fn.py
#
# Test MapReduce program functions used to test the mr.py library.
# See test_mapreduce.py for more.

def mapper(key,value,params): return [(0,(key+1)*(key+1))]

def reducer(key,value_list,params): return (key,sum(value_list))