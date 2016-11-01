import os
import datetime
import sys
import inspect
from qualikiz_tools.qualikiz_io.qualikizrun import QuaLiKizBatch, QuaLiKizRun
from qualikiz_tools.qualikiz_io.inputfiles import QuaLiKizPlan
from qualikiz_tools.machine_specific.slurm import Srun, Sbatch
import sqlite3
""" Creates a mini-job based on the reference example """
import csv
from collections import OrderedDict
from copy import deepcopy
from itertools import product, chain
import numpy as np

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
if os.path.isfile('jobdb.sqlite3'):
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
ncores = 1152
scan_plan = OrderedDict()
with open('10D database suggestion.csv') as csvfile:
    reader = csv.reader(csvfile)
    [next(reader) for x in range(3)]
    for row in reader:
        if row[0] == '':
           break
        print(row)
        scan_plan[row[1]] = [float(x) for x in row[4:] if x != '']
    print ('complete plan:' + str(scan_plan))

# Define the smallest 'chunck' of our run; the QuaLiKiz run
qualikiz_chunck = ['Ati', 'Ate', 'Ane', 'qx']
base_plan = QuaLiKizPlan.from_json('./parameters.json')
base_plan['scan_type'] = 'hyperrect'
base_plan['scan_dict'] = OrderedDict()
base_plan['xpoint_base']['meta']['seperate_flux'] = True
base_plan['xpoint_base']['norm']['Ani1'] = False
base_plan['xpoint_base']['norm']['ninorm1'] = True
base_plan['xpoint_base']['norm']['An_equal'] = True
base_plan['xpoint_base']['norm']['QN_grad'] = True
base_plan['xpoint_base']['norm']['recalc_Nustar'] = True
base_plan['xpoint_base']['norm']['recalc_Ti_Te_rel'] = True
base_plan['xpoint_base']['special']['kthetarhos'] = scan_plan.pop('kthetarhos')


for name in qualikiz_chunck:
    base_plan['scan_dict'][name] = scan_plan.pop(name)

scan_vip = {}
scan_vip['epsilon'] = 0.15
scan_vip['Ti_Te_rel'] = 1
scan_vip['Zeff'] = 1
scan_vip['Nustar'] = 1e-3
scan_vip['smag'] = 1
for key, item in scan_plan.items():
   scan_plan[key] = sorted(item, key=lambda x: (x - scan_vip[key])**2)
   print (scan_plan[key])

for key, item in scan_plan.items():
   if key == 'banana':
      scan_plan[key] = [scan_plan[key][0]]


# Now, we can put multiple runs in one batch script
batch_chunck = ['smag']
batch_variable = OrderedDict()
#set_item = {}
for name in batch_chunck:
    batch_variable[name] = scan_plan.pop(name)
    #set_item[name] = base_plan['xpoint_base'].howto_setitem(name)
    db.execute('''ALTER TABLE job ADD COLUMN ''' + name + ''' REAL''')
    

# We can now make a hypercube over all remaining parameters
#print ('hypercube over:' + str(*scan_plan.items()))
batchlist = []
for name in scan_plan:
    #set_item[name] = base_plan['xpoint_base'].howto_setitem(name)
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


    scan_point = OrderedDict(zip(scan_plan.keys(), scan_values))
    # Reorder elements in point list to avoid dependencies
    if (any([item in scan_point for item in ['epsilon', 'x', 'rho']]) and
        'Nustar' in scan_point):
       scan_point.move_to_end('Nustar')
    if all([item in scan_point for item in ['Nustar', 'Ti_Te_rel']]):
       scan_point.move_to_end('Ti_Te_rel')

    # Set xpoint_base to scan_point
    for name, value in scan_point.items():
        xpoint_base[name] = value
        batch_name += str(name) + str(value)

    # Sanity check: did setting work?
    for name, value in scan_point.items():
        if not np.isclose(xpoint_base[name], value, rtol=1e-4):
            raise Exception('Setting of ' + name + ' failed!' +
                            'Should be ' + str(value) + ' but is ' + str(xpoint_base[name]))
    # Now we have our 'base'. Each base has one batch with 10 runs inside
    joblist = []
    db.execute(insert_batch_string, (batch_id, queue_id, 0, 'initialized') + scan_values)
    
    for name, values in batch_variable.items():
        for value in values:
            db.execute(insert_job_string, (batch_id, 'initialized', value))
            job_name = str(name) + str(value)
            xpoint_base[name] = value
            job = QuaLiKizRun(os.path.join(rootdir, runsdir, batch_name), job_name, '../../../QuaLiKiz+pat', qualikiz_plan=base_plan_copy)
            joblist.append(job)
    batch = QuaLiKizBatch(os.path.join(rootdir, runsdir), batch_name, joblist, ncores, partition='regular')
    batchlist.append(batch)
    batch_id += 1
db.commit()
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    print (l.__class__)
    print (n.__class__)
    print (len(l).__class__)
    for i in range(0, len(l), n):
        yield l[i:i + n]
n = int(np.ceil(len(batchlist) / 24))
scan_chunks = chunks(batchlist, n)
scan_chunks = [x for x in scan_chunks]
#print (scan_chunks)

#resp = input('generate input? [Y/n]')
from multiprocessing import Process, Manager
import multiprocessing as mp
import pickle


def prepare(batch):
    batch.prepare(overwrite_batch=True)

def generate_input(batch):
    batch.generate_input()

#pool = mp.Pool(processes=4)
#print (pickledlist)
#pool.map(generate_input, batchlist)
for i, batch in enumerate(batchlist):
   print (str(i) + '/' + str(len(batchlist)))
   prepare(batch)

#for batch in batchlist:
#   generate_input(batch)

#resp = 'n'
#if resp == '' or resp == 'Y' or resp == 'y':
#   for i, batch in enumerate(batchlist):
#      batch.prepare(overwrite_batch=True)
#      batch.generate_input()
#      #batchsdir = batch.batchsdir
#      #name = batch.name
#      #batchdir = os.path.join(batchsdir, name)
#      #dumped_batch = QuaLiKizBatch.from_dir(batchdir)
#      #print (dumped_batch == batch)
#      print ('batch ' + str(i+1) + '/' + str(len(batchlist)))
#    #batch.queue_batch()
