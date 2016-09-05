import os
import datetime
import sys
import inspect
from qualikiz.qualikizrun import QuaLiKizBatch, QuaLiKizRun
from qualikiz.inputfiles import QuaLiKizPlan
from qualikiz.edisonbatch import Srun, Batch
""" Creates a mini-job based on the reference example """
import csv
from collections import OrderedDict
from copy import deepcopy
from itertools import product, chain

# Define some standard paths
runsdir = 'runs'
if len(sys.argv) == 2:
    if sys.argv[1] != '':
        runsdir = os.path.abspath(sys.argv[1])

rootdir =  os.path.abspath(
   os.path.join(os.path.abspath(inspect.getfile(inspect.currentframe())),
               '../..'))
print (rootdir)

scan_plan = OrderedDict()
with open('10D database suggestion.csv') as csvfile:
   reader = csv.reader(csvfile)
   rows = [x for x in reader]
   for row in rows[3:13]:
      print(row)
      scan_plan[row[1]] = [float(x) for x in row[4:] if x != '']
print (scan_plan)

# Define the smallest 'chunck' of our run; the QuaLiKiz run
qualikiz_chunck = ['Ati', 'Ate', 'Ane', 'qx']
base_plan = QuaLiKizPlan.from_json('./parameters.json')
base_plan['scan_dict'] = OrderedDict()
for name in qualikiz_chunck:
   base_plan['scan_dict'][name] = scan_plan.pop(name)
base_plan['kthetarhos'] = scan_plan.pop('kthetarhos')

# Now, we can put multiple runs in one batch script
batch_variable = OrderedDict()
batch_variable['smag'] = scan_plan.pop('smag')

# We can now make a hypercube over all remaining parameters
print (*scan_plan.values())
set_item = {}
batchlist = []
for name in chain(batch_variable, scan_plan):
   set_item[name] = base_plan['xpoint_base'].howto_setitem(name)
for scan_values in product(*scan_plan.values()):
   xpoint_base = deepcopy(base_plan['xpoint_base'])
   batch_name = ''
   for name, value in zip(scan_plan.keys(), scan_values):
      set_item[name](xpoint_base, value)
      batch_name += str(name) + str(value)
   # Now we have our 'base'. Each base has one batch with 10 runs inside
   joblist = []
   for name, range in batch_variable.items():
      for value in range:
         job_name = str(name) + str(value)
         set_item[name](xpoint_base, value)
         job = QuaLiKizRun(os.path.join(rootdir, runsdir, batch_name, job_name), '../../../QuaLiKiz', '../../../tools/qualikiz', stdout='job.stdout', stderr='job.stderr')
         joblist.append(job)
   batch = QuaLiKizBatch(os.path.join(rootdir, runsdir), batch_name, joblist)
   batchlist.append(batch)

resp = input('write job to file? [Y/n]')
if resp == '' or resp == 'Y' or resp == 'y':
    for job in joblist:
        job.prepare()
        job.generate_input()
    batch.generate_batchscript(24)
    batch.queue_batch()
