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

scan_plan = OrderedDict()
with open('10D database suggestion.csv') as csvfile:
   reader = csv.reader(csvfile)
   rows = [x for x in reader]
   for row in rows[3:13]:
      print(row)
      scan_plan[row[1]] = [float(x) for x in row[3:] if x != '']
print (scan_plan)

runsdir = 'runs'
if len(sys.argv) == 2:
    if sys.argv[1] != '':
        runsdir = os.path.abspath(sys.argv[1])

rootdir =  os.path.abspath(
   os.path.join(os.path.abspath(inspect.getfile(inspect.currentframe())),
               '../../'))
base_plan = QuaLiKizPlan.from_json('./parameters.json')
print (base_plan)
job = QuaLiKizRun(os.path.join(rootdir, runsdir, 'mini'), '../../QuaLiKiz', '../../tools/qualikiz',
                  stdout='job.stdout', stderr='job.stderr')
joblist = [job]
batch = QuaLiKizBatch(job.rundir, 'minibatch.sbatch', joblist)

resp = input('Submit job to queue? [Y/n]')
if resp == '' or resp == 'Y' or resp == 'y':
    for job in joblist:
        job.prepare()
        job.generate_input()
    batch.generate_batchscript(24)
    batch.queue_batch()
