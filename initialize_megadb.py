import os
import datetime
import sys
import inspect
from qualikiz.qualikizrun import QuaLiKizBatch, QuaLiKizRun
from qualikiz.edisonbatch import Srun, Batch
""" Creates a mini-job based on the reference example """

runsdir = 'runs'
if len(sys.argv) == 2:
    if sys.argv[1] != '':
        runsdir = os.path.abspath(sys.argv[1])

rootdir =  os.path.abspath(
   os.path.join(os.path.abspath(inspect.getfile(inspect.currentframe())),
                '../../'))
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
