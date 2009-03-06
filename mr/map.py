# map.py
#
# Part of the mr.py library.
#
# This program is automatically run on worker machines by mr.py, whenever a map
# job only is called. 

import itertools, mr_lib, sys

# Sets a flag, visible to the client, saying that the map phase on this worker 
# is not yet done.
mr_lib.set_flag("map_done",False)

# Extract the filename for the map job, and the name of the input dictionary
# it's to be run over.
filename,input_dict = sys.argv[1:]

# Get the parameters for the mapper
mapper_params = mr_lib.read_pickle("mapper_params.mr")

# Import the map job
module_name = filename[:-3]
exec("from "+module_name+" import mapper")

# Read the input dictionary into local memory
i = mr_lib.read_pickle(input_dict)

# Run the mapper over the input dictionary
for k in i.keys(): i[k] = mapper(k,i[k],mapper_params)

# Write the input dictionary back out
mr_lib.write_pickle(i,input_dict)

# Sets a flag, visible to the client, saying that the map phase on this worker
# is done.
mr_lib.set_flag("map_done",True)

