import sqlite3
from collections import OrderedDict
from IPython import embed
import os
import itertools
from warnings import warn
import shutil
import subprocess

jobdb = sqlite3.connect('jobdb.sqlite3')
folderbase = 'megarun_one_netcdf'
os.mkdir(folderbase)
jobdb.execute('''CREATE TABLE Archive_netcdf (
                  Path           TEXT,
                  Zeff           REAL,
                  Nustar         REAL,
                  Ti_Te_rel      REAL
              )''')
variables = OrderedDict([('Zeff'     , None),
                         ('Nustar'   , None),
                         ('Ti_Te_rel', None)])

for name in variables:
    query = jobdb.execute('''SELECT DISTINCT {} from Batch'''.format(name))
    el = query.fetchall()
    el_list = []
    for item in el:
        el_list.append(item[0])
    variables[name] = sorted(el_list)

megastring = 'hsi \''
for values in itertools.product(*variables.values()):
    folderstring = ''
    for el in zip(variables.keys(), values):
        folderstring = os.path.join(folderstring, str(el[0]) + str(el[1]))
    folderstring = os.path.join(folderbase, folderstring)
    #os.makedirs(folderstring)

    jobdb.execute('INSERT INTO Archive_netcdf VALUES (?, ?, ?, ?)',
                  tuple((folderstring, )) + tuple(values))
    os.makedirs(folderstring)

subprocess.call(['hsi \"put -PR ' + folderbase + ' ' + folderbase + '\"'], shell=True)
subprocess.call(['hsi \"chown -R karel:m2116 ' + folderbase + '\"'], shell=True)
jobdb.commit()
