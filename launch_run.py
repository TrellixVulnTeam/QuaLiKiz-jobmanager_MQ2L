import sqlite3
import warnings
from qualikiz_tools.qualikiz_io.qualikizrun import QuaLiKizBatch, QuaLiKizRun
import subprocess as sp
from warnings import warn
from IPython import embed

def prepare_input(db, amount, mode='ordered', batchid=None):
    if mode == 'ordered':
        query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared' LIMIT ?''', (str(amount),))
    elif mode == 'random':
        query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared' ORDER BY RANDOM() LIMIT ?''', (str(amount),))
    elif mode == 'specific':
        query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared' AND Id=? LIMIT ?''', (batchid, str(amount)))
    querylist = query.fetchall()
    print(querylist)
    for el in querylist:
        print (el)
        batchid = el[0]
        dir = el[1]
        #QuaLiKizBatch.generate_input
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            batch = QuaLiKizBatch.from_dir(dir)
        batch.generate_input()
        for i, run in enumerate(batch.runlist):
            if run.inputbinaries_exist():
                db.execute('''UPDATE Job SET State='inputed' WHERE Batch_id=? AND Job_id=?''',
                           (batchid, i))
                db.commit()
            else:
                raise Exception('Generation of input binaries failed')
        db.execute('''UPDATE Batch SET State='inputed' WHERE Id=?''',
                   (batchid,))
        db.commit()


def queue(db, amount):
    query = db.execute('''SELECT Id, Path FROM batch WHERE State='inputed' LIMIT ?''', (str(amount),))
    querylist = query.fetchall()
    print(querylist)
    for el in querylist:
        print(el)
        batchid = el[0]
        dir = el[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            batch = QuaLiKizBatch.from_dir(dir)
        jobnumber = batch.queue_batch()
        if jobnumber:
            db.execute('''UPDATE Batch SET State='queued', Jobnumber=? WHERE Id=?''',
                       (jobnumber, batchid))
            db.execute('''UPDATE Job SET State='queued' WHERE Batch_Id=?''',
                       (batchid,))

            db.commit()

def waiting_jobs():
    output = sp.check_output(['sqs'])
    lines = output.splitlines()
    return len(lines) - 1


def finished_check(db):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch WHERE State='queued' ''')
    querylist = query.fetchall()
    batch_notdone = 0
    for el in querylist:
        batchid = el[0]
        dir = el[1]
        jobnumber = el[2]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            batch = QuaLiKizBatch.from_dir(dir)
        output = sp.check_output(['sacct', '--brief', '--noheader', '--parsable2', '--job', str(jobnumber)])
        jobline = output.splitlines()[0]
        __, state, __ = jobline.split(b'|')
        print(state)
        if state == b'COMPLETED':
            batch_success = True
            for i, run in enumerate(batch.runlist):
                if run.is_done():
                    state = 'success'
                else:
                    state = 'failed'
                    batch_success = False
                db.execute('''UPDATE Job SET State=? WHERE Batch_id=? AND Job_id=?''',
                           (state, batchid, i))
                db.commit()
            if batch_success:
                state = 'success'
            else:
                state = 'failed'
            db.execute('''UPDATE Batch SET State=? WHERE Id=?''',
                           (state, batchid))
            db.commit()
        elif state.startswith(b'CANCELLED'):
            for i, run in enumerate(batch.runlist):
                if run.is_done():
                    db_state = 'success'
                else:
                    db_state = 'failed'
                db.execute('''UPDATE Job SET State=? AND Note='CANCELLED' WHERE Batch_id=? AND Job_id=?''',
                           (db_state, batchid, i))
                db.commit()
            db.execute('''UPDATE Batch SET State='cancelled' and Note='TIMEOUT' WHERE Id=?''',
                           (batchid,))
            db.commit()
        elif state == (b'TIMEOUT'):
            for i, run in enumerate(batch.runlist):
                if run.is_done():
                    db_state = 'success'
                else:
                    db_state = 'failed'
                db.execute('''UPDATE Job SET State=? AND Note='TIMEOUT' WHERE Batch_id=? AND Job_id=?''',
                           (db_state, batchid, i))
                db.commit()
            db.execute('''UPDATE Batch SET State='failed' AND Note='TIMEOUT' WHERE Id=?''',
                           (batchid,))
            db.commit()


        else:
            batch_notdone += 1
    print (str(batch_notdone) + ' not done')

import os
import tarfile
import shutil
def archive(db, limit):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch WHERE State='netcdfized' LIMIT ?''', (limit, ))
    querylist = query.fetchall()
    for el in querylist:
        print(el)
        batchid = el[0]
        batchdir = el[1]
        batchsdir, name = os.path.split(batchdir)
        netcdf_path = os.path.join(batchdir, name + '.nc')
        os.rename(netcdf_path, os.path.join(batchsdir, name + '.nc'))
        with tarfile.open(batchdir + '.tar', 'w') as tar:
            tar.add(batchdir, arcname=os.path.basename(batchdir))
        if os.path.isfile(batchdir + '.tar'):
            shutil.rmtree(batchdir)
            db.execute('''UPDATE Batch SET State='archived' WHERE Id=?''',
                      (batchid, ))
            db.commit()


def netcdfize(db, limit):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch WHERE State='success' LIMIT ?''', (limit,))
    querylist = query.fetchall()
    for el in querylist:
        print(el)
        batchid = el[0]
        dir = el[1]
        batch = QuaLiKizBatch.from_dir(dir)
        batch.to_netcdf()
        for i, run in enumerate(batch.runlist):
            print('Archiving ' + run.rundir)
            with tarfile.open(run.rundir + '.tar.gz', 'w:gz') as tar:
                tar.add(run.rundir, arcname=os.path.basename(run.rundir))
            if os.path.isfile(run.rundir + '.tar.gz'):
                shutil.rmtree(run.rundir)
                db.execute('''UPDATE Job SET State='archived' WHERE Batch_id=? AND Job_id=?''',
                           (batchid, i))
                db.commit()
        db.execute('''UPDATE Batch SET State='netcdfized' WHERE Id=?''', (batchid, ))
        db.commit()

def trash(db):
    resp = input('Warning: This operation is destructive! Are you sure? [Y/n]')
    if resp == '' or resp == 'Y' or resp == 'y':
        query = db.execute('''SELECT Id, Path from batch WHERE State='prepared' ''')
        querylist = query.fetchall()
        for el in querylist:
            print(el)
            batchid = el[0]
            dir = el[1]
            try:
                shutil.rmtree(dir)
            except FileNotFoundError:
                warn(dir + ' already gone')
            db.execute('''UPDATE Batch SET State='thrashed' WHERE Id=?''', (batchid, ))
            db.commit()



queuelimit = 1
db = sqlite3.connect('jobdb.sqlite3')
#in_queue = waiting_jobs()
#print (str(in_queue) + ' jobs in queue. Submitting ' + str(queuelimit-in_queue))
#prepare_input(db, 1, mode='specific', batchid=20)
#prepare_input(db, 1, mode='specific', batchid=400)
#prepare_input(db, 1, mode='specific', batchid=800)
#prepare_input(db, 1, mode='specific', batchid=1200)
#queue(db, 4)
finished_check(db)
#netcdfize(db, 2)
#embed()
#archive(db, 2)
#trash(db)
db.close()
