# distributed_pagerank.py

import mr

s = 0.85
n = 100 # number of pages

cluster = mr.cluster(1)

# set up a distributed hash describing the random web
cluster.create_dict("web.dict",xrange(n))
#cluster.map("initialize_web.py","web.dict",[n])
cluster.mr("test_mr.py",[],[],"web.dict","web.dict","test")

iteration = 1
change = 2 # initial estimate of error
tolerance = 1.0/n  # desired final bound on error

#while change > tolerance:
#  print "Iteration: "+str(iteration)

  # Run the MapReduce job used to compute the inner product
  # between the vector of dangling pages and the estimated
  # PageRank.
#  cluster.mr("ip",[n,s],[n,s],"web","web")
#  ip = cluster.get_value("ip_out",0)
  
  # Needed in case there are no dangling pages, in which case 
  # MapReduce returns ip as None.
#  if ip == None: ip = 0

  # Run the MapReduce job used to update the PageRank vector.
#  cluster.mr("pagerank",[],[n,s,ip],"web","web")

  # Compute the new estimate of error.
#  cluster.mr("error",[],[],"web","error_out")
#  change = cluster.get_value("error_out",1)

  # Update the estimated PageRank vector
#  cluster.mr("update",[],[],"web","web")
#  iteration += 1

# Shutdown the cluster
#cluster.shutdown