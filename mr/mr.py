# mr.py
#
# MapReduce library for Amazon EC2, by Michael Nielsen. 
#
# Developed under Python 2.4, with Ubuntu running on the client machine.
# Your mileage under other setups may vary.
#
# mr is a module which, when imported on a client machine, adds six
# basic capabilites:
#
# 1. The ability to create a cluster at Amazon EC2.
# 2. The ability to shut down a cluster.
# 3. The ability to create a distributed dictionary on the
#    cluster, with a specific set of keys.  The corresponding
#    values are themselves typically multi-key dictionaries.
#    Initially, they are set to the empty dictionary, {}.
# 4. The ability to retrieve a value from a distributed 
#    dictionary.
# 5. The ability to run a Map job on a distributed dictionary.
# 6. The ability to run a MapReduce job on a distributed dictionary.

import boto, cPickle, itertools, mr_lib, os, sys, thread, time

# Class to set up and manage clusters at Amazon EC2

class cluster:

  # Launch a cluster of size n.  Clusters have three attributes:
  #
  # cluster.size - number of machines in cluster
  # cluster.keypair - an boto keypair instance
  # cluster.workers - a boto reservation.
  #
  # Note that cluster.workers.instances is a list of machine
  # instances, in the boto instance class.

  def __init__(self,n):

    # set the size attribute
    self.size = n

    # set up an EC2 connection, and grab an Ubuntu 8.04 image (http://alestic.com)
    connection = boto.connect_ec2()
    image = connection.get_image("ami-1c5db975") 

    # create a keypair to use with the image, save to disk, and set permissions
    # so ssh will be happy
    self.keypair = connection.create_key_pair("mr_keypair")
    mr_lib.write_file(self.keypair.material,mr_lib.mr_keypair_filename())
    os.system("chmod 600 "+mr_lib.mr_keypair_filename())

    # tell EC2 to start the instances running, set the self.workers attribute to the 
    # corresponding reservation, and wait for all the workers to start running
    self.workers = image.run(n,n,"mr_keypair")
    for instance in self.workers.instances:
      instance.update()
      while instance.state != u'running':
        instance.update()
        time.sleep(5) 

    # Delay before we start distributing files, so all instances are running properly.
    time.sleep(10)

    # distribute a list of all the private ip addresses
    private_ip_list = [instance.private_dns_name for instances in self.workers.instances]
    mr_lib.write_pickle(private_ip_list,"cluster_description.mr")
    self.distribute_public("cluster_description.mr")
    for j in xrange(n):
      mr_lib.write_pickle([j,self.workers.instances[j].private_dns_name],"my_details.mr")
      self.send("my_details.mr",j)

    # distribute the files necessary to run map and mapreduce jobs
    self.distribute_public("map.py")
    self.distribute_public("map_combine.py")
    self.distribute_public("reduce.py")
    self.distribute_public("mr_lib.py")

    # Distribute the ssh keypairs and config file
    for instance in self.workers.instances:
      mr_lib.scp(mr_lib.mr_keypair_filename(),"root@"+instance.public_dns_name+":.ssh/id_rsa-mr_keypair")
      mr_lib.ssh("root@"+instance.public_dns_name,"chmod 600 /root/.ssh/id_rsa-mr_keypair")
      mr_lib.scp(os.environ.get("HOME")+"/.ssh/config","root@"+instance.public_dns_name+":.ssh/config")

  # shuts down the cluster and deletes the ssh keypair

  def shutdown(self):
    self.workers.stop_all()
    self.keypair.delete()
    os.system("rm "+mr_lib.mr_keypair_filename())


  # set up a distributed dictionary, with a given key set.

  def create_dict(self,name,keys):
    worker_key_pairs = sorted([(hash(key) % self.size,key) for key in keys])
    for machine, local_group in itertools.groupby(sorted(worker_key_pairs),lambda x: x[0]):
      local_dict = {}
      for machine,key in local_group: local_dict[key] = {}
      mr_lib.write_pickle(local_dict,name)
      self.send(name,machine)


  # Returns the value associated to key, for the distributed dictionary dict_name.

  def get_dict_value(self,dict_name,key):
    instance = self.workers.instances[hash(key) % self.size]
    instance_filename = "root@"+instance.public_dns_name+":"+dict_name
    mr_lib.scp(instance_filename,dict_name)
    d = mr_lib.read_pickle(dict_name)
    value = d[key]
    os.remove(dict_name)
    return value
  

  # send the file filename to worker number worker_number

  def send(self,filename,worker_number):
    instance = self.workers.instances[worker_number]
    instance_filename = "root@"+instance.public_dns_name+":"+filename
    mr_lib.scp(filename,instance_filename)


  # send the file filename to every machine in the cluster, using the EC2
  # public IP address.

  def distribute_public(self,filename):
    for instance in self.workers.instances:
      instance_filename = "root@"+instance.public_dns_name+":"+filename
      mr_lib.scp(filename,instance_filename)


  # runs cmd on every machine in the cluster
  def exec_public(self,cmd):
    for instance in self.workers.instances:
      instance_name = "root@"+instance.public_dns_name
      thread.start_new_thread(mr_lib.ssh(instance_name,),)
 
  # Runs the mapreduce job. mapper and reducer functions must be defined in the file
  # filename.  mapper is passed mapper_params; reducer is passed reducer_params.
  # The input dictionary has name input_dict, while the intermediate (and
  # output) dictionary has name output_dict.
  
  def mr(self,filename,mapper_params,reducer_params,input_dict,output_dict,output_field):
    # distribute the file defining the mapper and reducer functions through the cluster,
    # as well as the corresponding parameters
    self.distribute_public(filename)
    mr_lib.write_pickle([mapper_params,reducer_params],"params.mr")
    self.distribute_public("params.mr")

    # Launch the map phase, with combining, and wait until it's done
    self.exec_public("python map_combine.py "+filename+" "+input_dict)
    self.wait_until_task_done("map_combine_done")

    # Launch the reduce phase, and wait until it's done
    self.exec_public("python reduce.py "+filename+" "+output_dict+" "+output_field)
    self.wait_until_task_done("reduce_done")

  def map(self,filename,input_dict,mapper_params):
    # distribute the file defining the mapper functions through the cluster,
    # as well as the corresponding parameters.
    self.distribute_public(filename)
    mr_lib.write_pickle(mapper_params,"mapper_params.mr")
    self.distribute_public("mapper_params.mr")

    # Launch the map phase, and wait until it's done.
    self.exec_public("python map.py "+filename+" "+input_dict)
    self.wait_until_task_done("map_done")

  def wait_until_task_done(self,flag_name):
    done = False
    while not done:
      time.sleep(2)
      done = True
      for instance in self.workers.instances:
        mr_lib.scp("root@"+instance.public_dns_name+":flags.mr","flags.mr")
        if not mr_lib.get_flags()[flag_name]: 
          print instance.public_dns_name+" not done."
          done = False
        else:
          print instance.public_dns_name+" done."
