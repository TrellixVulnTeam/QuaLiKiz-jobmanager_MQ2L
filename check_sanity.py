import sqlite3
import glob
import warnings
from qualikiz_tools.qualikiz_io.qualikizrun import QuaLiKizBatch
import os
db = sqlite3.connect('jobdb.sqlite3')
query = db.execute('''SELECT Id, Path, Jobnumber, State FROM batch''')
                   
querylist = query.fetchall()
changed_els = []
notfixed_els = []
for el in querylist:
    print('Sanity checking ' + str(el))
    batchid = el[0]
    batchdir = el[1]
    jobnumber = el[2]
    state = el[3]
    if state == 'success':
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                batch = QuaLiKizBatch.from_dir(batchdir)
            except Exception as e:
                if e.args[0] == 'Could not find run':
                    print(str(el) + ' is insane! Checking if netcdfized')
                    if os.path.exists(os.path.join(batchdir, os.path.basename(batchdir) + '.nc')):
                        print('NetCDF exists')
                        tarred_files = glob.glob(batchdir + '/*.tar.gz')
                        jobquery = db.execute('''SELECT Job_id from Job WHERE Batch_id==?''', (batchid, ))
                        jobid_list = jobquery.fetchall()
                        print(str(len(tarred_files)) + ' tarred files of ' + str(len(jobid_list)) + ' jobs')
                        if len(tarred_files) == len(jobid_list):
                            print('Probably netcdfized. Changing state')
                            db.execute('''UPDATE Batch SET State='netcdfized' WHERE Id=?''', (batchid, ))
                            db.execute('''UPDATE Job SET State='netcdfized'
                                          WHERE Batch_id=?''',
                                          (batchid, ))
                            db.commit()
                            changed_els.append(el)
                        elif len(tarred_files) > 0:
                            notfixed_els.append({'el': el, 'strat': 'targz'})
                        else:
                            notfixed_els.append({'el': el, 'strat': 'check netcdf'})

                    else:
                        print('Not netcdfized. Give up! Please check manually')
                        notfixed_els.append(el)
                else:
                    raise
            else:
                batch_success = True
                for i, run in enumerate(batch.runlist):
                    if run.is_done():
                        pass
                    else:
                        batch_success = False
                if not batch_success:
                    print(str(el) + ' is insane!')
print()
print('Changed ids:')
for el in changed_els:
    print(el)
print()
print('Not fixed ids:')
with open('sanity_check_nonfixed', 'w') as file_:
    for dict_ in notfixed_els:
        print(dict_)
        file_.write(dict_['strat'] + ',' + dict_['el'][1] + '\n')
print('Script done')
