import os
import datetime
import sys
import inspect
from qualikiz.qualikizrun import QuaLiKizBatch, QuaLiKizRun
from qualikiz.inputfiles import QuaLiKizPlan
from qualikiz.edisonbatch import Srun, Batch
import sqlite3
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

# And initialize the megadb job database
os.remove('jobdb.sqlite3')
db = sqlite3.connect('jobdb.sqlite3')
db.execute('''CREATE TABLE queue (
                Id             INTEGER PRIMARY KEY
           )''')
db.execute('''CREATE TABLE batch (
                Id             INTEGER PRIMARY KEY,
                Queue_id       INTEGER,
                Jobnumber      INTEGER,
                State          TEXT
            )''')
db.execute('''CREATE TABLE job (
                Batch_id       INTEGER,
                State          TEXT
            )''')

# Define the amount of cores to be used
ncores = 24
scan_plan = OrderedDict()
with open('10D database suggestion.csv') as csvfile:
    reader = csv.reader(csvfile)
    rows = [x for x in reader]
    for row in rows[3:10]:
        print(row)
        scan_plan[row[1]] = [float(x) for x in row[4:] if x != '']
    print ('complete plan:' + str(scan_plan))

# Define the smallest 'chunck' of our run; the QuaLiKiz run
qualikiz_chunck = ['Ati', 'Ate', 'Ane', 'qx']
base_plan = QuaLiKizPlan.from_json('./parameters.json')
base_plan['scan_dict'] = OrderedDict()
for name in qualikiz_chunck:
    base_plan['scan_dict'][name] = scan_plan.pop(name)
base_plan['xpoint_base']['special']['kthetarhos'] = scan_plan.pop('kthetarhos')

# Now, we can put multiple runs in one batch script
batch_chunck = ['smag']
batch_variable = OrderedDict()
set_item = {}
for name in batch_chunck:
    batch_variable[name] = scan_plan.pop(name)
    set_item[name] = base_plan['xpoint_base'].howto_setitem(name)
    db.execute('''ALTER TABLE job ADD COLUMN ''' + name + ''' REAL''')
    

# We can now make a hypercube over all remaining parameters
print ('hypercube over:' + str(*scan_plan.items()))
batchlist = []
for name in scan_plan:
    set_item[name] = base_plan['xpoint_base'].howto_setitem(name)
    db.execute('''ALTER TABLE batch ADD COLUMN ''' + name + ''' REAL''')

# Initialize some database stuff
insert_job_string = '''INSERT INTO job VALUES (?, ? ''' + str(', ?'*(len(batch_chunck))) + ''')'''
insert_batch_string = '''INSERT INTO batch VALUES (?, ?, ?, ? ''' + str(', ?'*(len(scan_plan))) + ''')'''
                                                      
queue_id = 0
batch_id = 0
for scan_values in product(*scan_plan.values()):
    base_plan_copy = deepcopy(base_plan)
    xpoint_base = base_plan_copy['xpoint_base']
    batch_name = ''
    value_list = []
    for name, value in zip(scan_plan.keys(), scan_values):
        set_item[name](xpoint_base, value)
        batch_name += str(name) + str(value)
    # Now we have our 'base'. Each base has one batch with 10 runs inside
    joblist = []
    db.execute(insert_batch_string, (batch_id, queue_id, 0, 'initialized') + scan_values)
    
    for name, range in batch_variable.items():
        for value in range:
            db.execute(insert_job_string, (batch_id, 'initialized', value))
            job_name = str(name) + str(value)
            set_item[name](xpoint_base, value)
            job = QuaLiKizRun(os.path.join(rootdir, runsdir, batch_name), job_name, '../../../QuaLiKiz', '../../../tools/qualikiz', qualikiz_plan=base_plan_copy, stdout='job.stdout', stderr='job.stderr')

            joblist.append(job)
    batch = QuaLiKizBatch(os.path.join(rootdir, runsdir), batch_name, joblist, ncores)
    batchlist.append(batch)
    batch_id += 1

db.commit()
resp = input('generate input? [Y/n]')
if resp == '' or resp == 'Y' or resp == 'y':
   for i, batch in enumerate(batchlist):
      batch.prepare(overwrite_batch=True)
      batch.generate_input()
      batchsdir = batch.batchsdir
      name = batch.name
      batchdir = os.path.join(batchsdir, name)
      dumped_batch = QuaLiKizBatch.from_dir(batchdir)
      print (dumped_batch == batch)
      print ('batch ' + str(i) + '/' + str(len(batchlist)))
    #batch.queue_batch()
