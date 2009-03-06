# mr_lib.py
#
# Part of the mr.py library.
#
# Utility module imported by many of the programs used in the library.

import cPickle,os

def read_file(filename):
  if os.path.exists(filename):
    file = open(filename,"r")
    value = file.read()
    file.close()
  else:
    value = ""
  return value

def write_file(string,filename):
  file = open(filename,"w")
  file.write(string)
  file.close()

def read_pickle(filename):
  if os.path.exists(filename):
    file = open(filename,"r")
    obj = cPickle.load(file)
    file.close()
  else:
    obj = None
  return obj

def write_pickle(obj,filename):
  file = open(filename,"w")
  cPickle.dump(obj,file)
  file.close()

def write_and_send_pickle(obj,filename,ip):
  write_pickle(obj,filename)
  send(filename,ip)

def send(filename,ip):
  instance_filename = "root@"+ip+":"+filename
  os.system("scp -q -i /root/.ssh/id_rsa-mr_keypair "+filename+" "+instance_filename)

def set_flag(flag,value):
  flags = get_flags()
  flags[flag] = value
  write_pickle(flags,"flags.mr")

def get_flags():
  flags = read_pickle("flags.mr")
  if flags == None: flags = {}
  return flags

# filename for the ssh key used by the cluster
def mr_keypair_filename():
  return os.environ.get("HOME")+"/.ssh/id_rsa-mr_keypair"

def scp(local_filename,remote_filename):
  os.system("scp -q -i "+mr.mr_keypair_filename()+" "+local_filename+" "+remote_filename)


def ssh(who,cmd):
  os.system("ssh -q -i "+mr.mr_keypair_filename()+" "+who+" "+cmd)
