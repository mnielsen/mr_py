# map_combine.py
#
# Part of the mr.py library.
#
# This program is automatically run on worker machines by mr.py, at the start of
# a MapReduce job.

import itertools, mr_lib, sys

# Sets a flag, visible to the client, saying that the map_combine phase on this worker 
# is not yet done.
mr_lib.set_flag("map_combine_done",False)

# Read in ip address of current worker, and description of whole cluster
my_number,my_ip = mr_lib.read_pickle("my_details.mr")
ip = mr_lib.read_pickle("cluster_description.mr")

# Read the filename and input dictionary
filename,input_dict = sys.argv[1:]

# Get the parameters for the MapReduce job
mapper_params,reducer_params = mr_lib.read_pickle("params.mr")

# import the mapper and reducer
module = filename[:-3]
exec("from "+module+" import mapper, reducer")

# Read the input dictionary into local memory
i = mr_lib.read_pickle(input_dict)

# Run MapReduce over the local input dictionary.
intermediate = []
for (key,value) in i.items():
  intermediate.extend(mapper(key,value,mapper_params))
groups = {}
for key, group in itertools.groupby(sorted(intermediate), 
                                    lambda x: x[0]):
  groups[key] = list([y for x, y in group])
inter_result = [reducer(inter_key,groups[inter_key],reducer_params) for inter_key in groups] 

distribution_triples = sorted([(hash(key) % len(ip),inter_key,inter_value) 
                                for inter_key,inter_value in inter_result])
for machine, group in itertools.groupby(sorted(distribution_triples),lambda x: x[0]):
  local_group = list([(y,z) for x, y, z in group])
  name = "inter.dict."+str(my_number)+"."+str(machine)
  mr_lib.write_and_send_pickle(local_group,name,ip[machine])

# Sets a flag, visible to the client, saying that the map phase on this worker
# is done.
mr_lib.set_flag("map_combine_done",True)
