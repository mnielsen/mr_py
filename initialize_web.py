# initialize_web.py
#

import random

def mapper(key,value,params):
  n = params[0]
  d = {}
  d["prob"] = 1.0/n
  l = random.randint(0,20)
  d["num_out_links"] = l
  d["out_links"] = random.sample(xrange(n),l)
  return d