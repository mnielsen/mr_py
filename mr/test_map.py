# test_map.py
#
# Used to test the mr.py library's Map function.
#
# The program has two parameters: n, and cluster_size, defined in the first
# two lines of code.  The program sets up a distributed dictionary with
# 1 through n as keys, and then uses a Map job, running on cluster_size
# machines, to square everything in the dictionary.

n = 500
cluster_size = 2

import mr
import random
cluster = mr.cluster(cluster_size)
cluster.create_dict("integers.dict",xrange(n))
cluster.map("test_map_fn.py","integers.dict",[])

print "Testing the Map function of mr.py.  This programs generates integers from"
print "0 to "+str(n)+" and squares them."
test_results, desired_results = [], []
for j in xrange(5):
  key = random.randint(0,n-1)
  test_results.append(cluster.get_dict_value("integers.dict",key))
  desired_results.append(key**2)
print "Five random results from the computed dictionary:", str(test_results)
print "Actual desired results (should match):           ", str(desired_results)

cluster.shutdown()