# map.py
#
# Part of the mr.py library.  Run on worker machines by mr.py, when a map
# job only is called. 

import itertools, mr_lib, sys

mr_lib.set_flag("map_done",False) # map phase on this worker not yet done
filename,input_dict = sys.argv[1:]
module_name = filename[:-3]
exec("from "+module_name+" import mapper") # import map job
i = mr_lib.read_pickle(input_dict) # Read the input dictionary
mapper_params = mr_lib.read_pickle("mapper_params.mr") # Get the parameters for the mapper
for k in i.keys(): i[k] = mapper(k,i[k],mapper_params) # Run the mapper
mr_lib.write_pickle(i,input_dict) # Write the input dictionary back out
mr_lib.set_flag("map_done",True) # map phase is done

