# reduce.py
#
# Part of the mr.py library.
#
# This program is automatically run on worker machines by mr.py, to conclude
# a MapReduce job.

import itertools, mr_lib, os, sys

# Sets a flag, visible to the client, saying that the reduce phase on this worker 
# is not yet done.
mr_lib.set_flag("reduce_done",False)

# Read in ip address of current worker, and description of whole cluster
my_number,my_ip = mr_lib.read_pickle("my_details.mr")
ip = mr_lib.read_pickle("cluster_description.mr")

# Read the filename and input dictionary
filename,output_dict,output_field = sys.argv[1:]

# Get the parameters for the MapReduce job
mapper_params,reducer_params = mr_lib.read_pickle("params.mr")

# import the reducer
module = filename[:-3]
exec("from "+module+" import reducer")

# read in all the intermediate data
intermediate = []
for machine in xrange(len(ip)):
  name = "inter.dict."+str(machine)+"."+str(my_number)
  if os.path.exists(name): intermediate.extend(mr_lib.read_pickle(name))

groups = {}
for key, group in itertools.groupby(sorted(intermediate), 
                                    lambda x: x[0]):
  groups[key] = list([y for x, y in group])
result = [reducer(inter_key,groups[inter_key],reducer_params) for inter_key in groups] 

# load the existing output dictionary, and modify it to include the results
# obtained in the reduce step, saving the resulting file.
o = mr_lib.read_pickle(output_dict)
for key, value in result:  o[key][output_field] =  value
mr_lib.write_pickle(o,output_dict)

# Sets a flag, visible to the client, saying that the reduce phase on this worker
# is done.
mr_lib.set_flag("reduce_done",True)
