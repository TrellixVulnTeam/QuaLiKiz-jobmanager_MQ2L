"""
Copyright Dutch Institute for Fundamental Energy Research (2016)
Contributors: Karel van de Plassche (karelvandeplassche@gmail.com)
License: CeCILL v2.1

This script generates the skeleton needed to set up a large QuaLiKiz collection of
runs. A sqlite3 database with job information is generated, and the other scripts
in this folder use this database to manage, run, initialize and archive the QuaLiKiz
jobs.
"""
import os
import sys
import inspect
import sqlite3
import csv
from collections import OrderedDict
from copy import deepcopy
from itertools import product, chain           

from IPython import embed #pylint: disable=unused-import
import numpy as np

from qualikiz_tools.qualikiz_io.qualikizrun import QuaLiKizBatch, QuaLiKizRun
from qualikiz_tools.qualikiz_io.inputfiles import QuaLiKizPlan

# Define some standard paths
runsdir = 'runs'
if len(sys.argv) == 2:
    if sys.argv[1] != '':
        runsdir = os.path.abspath(sys.argv[1])

rootdir = os.path.abspath(
    os.path.join(os.path.abspath(inspect.getfile(inspect.currentframe())),
                 '../..'))
print(rootdir)

# And initialize the megadb job database
if os.path.isfile('jobdb.sqlite3'):
    raise Exception('Error: jobdb.sqlite3 exists!')
db = sqlite3.connect('jobdb.sqlite3')
db.execute('''CREATE TABLE queue (
                Id             INTEGER PRIMARY KEY
           )''')
db.execute('''CREATE TABLE batch (
                Id             INTEGER PRIMARY KEY,
                Queue_id       INTEGER,
                Jobnumber      INTEGER,
                Path           TEXT,
                State          TEXT,
                Note           TEXT
            )''')
db.execute('''CREATE TABLE job (
                Batch_id       INTEGER,
                Job_id         INTEGER,
                State          TEXT,
                Note           TEXT
            )''')

# Define the amount of cores to be used
ncores = 1152

# Load the scan plan from csv. This is based on the 10D database google sheet
scan_plan = OrderedDict()
with open('10D database suggestion.csv') as csvfile:
    reader = csv.reader(csvfile)
    [next(reader) for x in range(3)] #pylint: disable=expression-not-assigned
    for row in reader:
        if row[0] == '':
            break
        scan_plan[row[1]] = [float(x) for x in row[4:] if x != '']

print('Read CSV:')
for name, values in scan_plan.items():
    print('{:<12}'.format(name) + ' ' + str(values))

# Use the parameters.json as base for our hypercube
base_plan = QuaLiKizPlan.from_json('./parameters.json')


# Start initializing the scan_dict. Start with kthetarhos
base_plan['scan_dict'] = OrderedDict()
kthetarhos = scan_plan.pop('kthetarhos')
base_plan['xpoint_base']['special']['kthetarhos'] = kthetarhos


# Define the smallest 'chunck' of our run; the QuaLiKiz run
qualikiz_chunck = ['At', 'An', 'qx', 'smag']
for name in qualikiz_chunck:
    base_plan['scan_dict'][name] = scan_plan.pop(name)

print('\n\rSmallest chunk:')
for name, values in base_plan['scan_dict'].items():
    print('{:<12}'.format(name) + ' ' + str(values))

# Sort the remaining parameters based on LMS difference from center value
scan_vip = {}
scan_vip['epsilon'] = 0.15
scan_vip['Ti_Te_rel'] = 1
scan_vip['Zeff'] = 1
scan_vip['Nustar'] = 1e-3
scan_vip['smag'] = 1
for key, vip_value in scan_vip.items():
    scan_plan[key] = sorted(scan_plan[key], key=lambda x: (x - vip_value)**2) #pylint: disable=cell-var-from-loop

# Debugging: Remove all but one value for scan_plan
#for key, item in scan_plan.items():
#   if key in  ['Ti_Te_rel', 'Zeff', 'Nustar', 'epsilon'] :
#      scan_plan[key] = [scan_plan[key][0]]


# Now, we can put multiple runs in one batch script
batch_chunck = ['Zeff']
batch_variable = OrderedDict()
for name in batch_chunck:
    batch_variable[name] = scan_plan.pop(name)
    db.execute('''ALTER TABLE job ADD COLUMN ''' + name + ''' REAL''')

print('\n\rBatch:')
for name, values in batch_variable.items():
    print('{:<12}'.format(name) + ' ' + str(values))


# We can now make a hypercube over all remaining parameters
# Lets reverse the list so the priority is correct
scan_plan = OrderedDict(reversed(list(scan_plan.items())))

batchlist = []
print('\n\rScan:')
for name, values in scan_plan.items():
    print('{:<12}'.format(name) + ' ' + str(values))
    db.execute('''ALTER TABLE batch ADD COLUMN ''' + name + ''' REAL''')
print()

# Initialize some database stuff
insert_job_string = ('''INSERT INTO job VALUES (?, ?, ?, ? ''' +
                     str(', ?'*(len(batch_chunck))) + ''')''')
insert_batch_string = ('''INSERT INTO batch VALUES (?, ?, ?, ?, ?, ? ''' +
                       str(', ?'*(len(scan_plan))) + ''')''')

queue_id = 0
batch_id = 0
tot = np.product([len(x) for x in scan_plan.values()])
for i, scan_values in enumerate(product(*scan_plan.values())):
    print(str(i) + '/' + str(tot))
    #base_plan_parent_copy = deepcopy(base_plan)
    #xpoint_base = base_plan_parent_copy['xpoint_base']
    batch_name = ''
    value_list = []

    joblist = []
    scan_point = OrderedDict(zip(scan_plan.keys(), scan_values))
    for name, value in scan_point.items():
        batch_name += str(name) + str(value)

    for name, values in batch_variable.items():
        for i, value in enumerate(values):
            base_plan_batch_copy = deepcopy(base_plan)
            xpoint_base = base_plan_batch_copy['xpoint_base']
            scan_point[name] = value
            job_name = str(name) + str(value)
            print(job_name)
            # Reorder elements in point list to avoid dependencies
            if (any([item in scan_point for item in ['epsilon', 'x', 'rho']]) and
                    'Nustar' in scan_point):
                scan_point.move_to_end('Nustar')
            if all([item in scan_point for item in ['Nustar', 'Ti_Te_rel']]):
                scan_point.move_to_end('Ti_Te_rel')

            # Set xpoint_base to scan_point
            for scan_name, scan_value in scan_point.items():
                xpoint_base[scan_name] = scan_value

            # Now we have our 'batchbase'. Each batch has a unique base
            # of which we will create 10 runs
            
            # Sanity check: did setting work?
            print(job_name)
            for scan_name, scan_value in scan_point.items():
                print(scan_name, scan_value)
                if not np.isclose(xpoint_base[scan_name], scan_value, rtol=1e-4):
                    raise Exception('Setting of ' + scan_name + ' failed!' +
                                    'Should be ' + str(scan_value) + ' but is ' + str(xpoint_base[scan_name]))
            db.execute(insert_job_string, (batch_id, i, 'prepared', None, value))
            # If you want to run a different binary, change here
            job = QuaLiKizRun(os.path.join(rootdir, runsdir, batch_name),
                              job_name, '../../../QuaLiKiz',
                              qualikiz_plan=base_plan_batch_copy)
            joblist.append(job)
    # If you want a different QOS or repo, change here
    batch = QuaLiKizBatch(os.path.join(rootdir, runsdir), batch_name,
                          joblist, ncores, partition='regular', repo='m64')
    batch.prepare(overwrite_batch=True)
    db.execute(insert_batch_string, (batch_id, queue_id, 0,
                                     os.path.join(batch.batchsdir, batch.name),
                                     'prepared', None) + scan_values)
    batchlist.append(batch)
    batch_id += 1
db.commit()

# Legacy: if we want to do parallel generation of input
#def chunks(l, n):
#    """Yield successive n-sized chunks from l."""
#    for ii in range(0, len(l), n):
#        yield l[i:i + n]
#numchunks = int(np.ceil(len(batchlist) / 24))
#scan_chunks = chunks(batchlist, numchunks)
#scan_chunks = [x for x in scan_chunks]
##print (scan_chunks)
#
##resp = input('generate input? [Y/n]')
#from multiprocessing import Process, Manager
#import multiprocessing as mp
#import pickle
#
#
#
#def generate_input(batch):
#    batch.generate_input()
#
#pool = mp.Pool(processes=4)
#pool.map(generate_input, batchlist)

# Legacy: Generate all input. Takes long!
#for batch in batchlist:
#   generate_input(batch)
