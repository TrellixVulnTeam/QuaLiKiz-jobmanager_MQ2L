"""
Copyright Dutch Institute for Fundamental Energy Research (2016)
Contributors: Karel van de Plassche (karelvandeplassche@gmail.com)
License: CeCILL v2.1
"""
# Suggestion: Run with python launch_run.py > launch_run.py.log 2>&1 &
import os
if __name__ == '__main__':
    lockfile = os.path.abspath('launch_run.py.lock')
    if os.path.exists(lockfile):
        exit('Lock file exists')
    with open(lockfile, 'w') as lock:
        pass
import sqlite3
import warnings
from warnings import warn
import subprocess as sp
import tarfile
import shutil
import sys
import time
import pwd
import grp
import subprocess
import glob
import os

from IPython import embed #pylint: disable=unused-import

from qualikiz_tools.qualikiz_io.qualikizrun import QuaLiKizBatch

def prepare_input(db, amount, mode='ordered', batchid=None):
    if mode == 'ordered':
        query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared'
                           LIMIT ?''', (str(amount),))
    elif mode == 'random':
        query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared'
                           ORDER BY RANDOM() LIMIT ?''', (str(amount),))
    elif mode == 'specific':
        query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared'
                           AND Id=? LIMIT ?''', (batchid, str(amount)))
    querylist = query.fetchall()
    for el in querylist:
        print('generating input for: ' + str(el))
        batchid = el[0]
        batchdir = el[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            batch = QuaLiKizBatch.from_dir(batchdir)
        batch.generate_input()
        for i, run in enumerate(batch.runlist):
            if run.inputbinaries_exist():
                db.execute('''UPDATE Job SET State='inputed'
                           WHERE Batch_id=? AND Job_id=?''',
                           (batchid, i))
                db.commit()
            else:
                raise Exception('Error: Generation of input binaries failed')
        db.execute('''UPDATE Batch SET State='inputed' WHERE Id=?''',
                   (batchid,))
        db.commit()


def queue(db, amount):
    query = db.execute('''SELECT Id, Path FROM batch WHERE State='inputed'
                       LIMIT ?''', (str(amount),))
    querylist = query.fetchall()
    for el in querylist:
        print('queueing: ' + str(el))
        batchid = el[0]
        batchdir = el[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            batch = QuaLiKizBatch.from_dir(batchdir)
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
        print('Checking ' + str(el))
        batchid = el[0]
        batchdir = el[1]
        jobnumber = el[2]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            batch = QuaLiKizBatch.from_dir(batchdir)
        output = sp.check_output(['sacct',
                                  '--brief', '--noheader', '--parsable2',
                                  '--job', str(jobnumber)])
        try:
            jobline = output.splitlines()[0]
        except IndexError:
            print('Something went wrong!')
            print(output)

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
                db.execute('''UPDATE Job SET State=?, Note='Unknown' WHERE Batch_id=? AND Job_id=?''',
                           (state, batchid, i))
                db.commit()
            if batch_success:
                state = 'success'
            else:
                state = 'failed'
            db.execute('''UPDATE Batch SET State=?, Note='Unknown' WHERE Id=?''',
                       (state, batchid))
            db.commit()
        elif state.startswith(b'CANCELLED'):
            for i, run in enumerate(batch.runlist):
                if run.is_done():
                    db_state = 'success'
                else:
                    db_state = 'failed'
                db.execute('''UPDATE Job SET State=?, Note='CANCELLED'
                           WHERE Batch_id=? AND Job_id=?''',
                           (db_state, batchid, i))
                db.commit()
            db.execute('''UPDATE Batch SET State='failed', Note='CANCELLED'
                       WHERE Id=?''', (batchid,))
            db.commit()
        elif state == (b'TIMEOUT'):
            for i, run in enumerate(batch.runlist):
                if run.is_done():
                    db_state = 'success'
                else:
                    db_state = 'failed'
                db.execute('''UPDATE Job SET State=?, Note='TIMEOUT'
                           WHERE Batch_id=? AND Job_id=?''',
                           (db_state, batchid, i))
                db.commit()
            db.execute('''UPDATE Batch SET State='failed', Note='TIMEOUT'
                       WHERE Id=?''', (batchid,))
            db.commit()

        else:
            batch_notdone += 1
    print(str(batch_notdone) + ' not done')

def archive_el(db, el):
    print('Archiving: ' + str(el))
    batchid = el[0]
    batchdir = el[1]
    Zeff = el[2]
    Nustar = el[3]
    Ti_Te_rel = el[4]

    query = db.execute('''SELECT Path FROM Archive_netcdf
                          WHERE Zeff==? AND Nustar==? AND Ti_Te_rel==?''',
                       (Zeff, Nustar, Ti_Te_rel))
    
    hsi_basepath = query.fetchall()[0][0]
    batchsdir, name = os.path.split(batchdir)
    netcdf_hsi_path = os.path.join(hsi_basepath, name + '.nc')

    netcdf_path_current = os.path.join(batchdir, name + '.nc')
    netcdf_path_target = os.path.join(batchsdir, name + '.nc')
    # We'll leave a copy behind
    os.rename(netcdf_path_current, netcdf_path_target)
    subprocess.check_call(['hsi \"put ' + netcdf_path_target + ' : ' + netcdf_hsi_path + '\"'], shell=True)
    subprocess.check_call(['hsi \"chown karel:m2116 ' + netcdf_hsi_path+ '\"'], shell=True)

    # Now archive the folder
    archive_hsi_base = 'megarun_one_archive/'
    archive_hsi_path = os.path.join(archive_hsi_base, os.path.basename(batchdir) + '.tar')
    subprocess.check_call(['htar -cf ' + archive_hsi_path + ' ' + batchdir + ' -C ' + batchsdir], shell=True)
    subprocess.check_call(['hsi \"chown karel:m2116 ' + archive_hsi_path + ' ' + archive_hsi_path + '.idx\"'], shell=True)

    db.execute('''UPDATE Batch SET State='archived' WHERE Id=?''',
                   (batchid, ))
    db.commit()

def archive(db, limit):
    query = db.execute('''SELECT Id, Path, Zeff, Nustar, Ti_Te_rel FROM batch
                       WHERE State='netcdfized' LIMIT ?''', (limit, ))
    querylist = query.fetchall()
    for el in querylist:
        archive_el(db, el)


def clean_success(db, criteria):
    resp = input('Warning: This operation is destructive! Are you sure? [y/N]')
    if resp == 'Y' or resp == 'y':
        query = db.execute('''SELECT Id, Path, Jobnumber FROM batch
                           WHERE State='success' AND ''' + criteria) 
        querylist = query.fetchall()
        for el in querylist:
            print('Cleaning ' + str(el))
            batchid = el[0]
            batchdir = el[1]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                batch = QuaLiKizBatch.from_dir(batchdir)
            batch.clean()
            db.execute('''UPDATE Batch SET State='prepared' WHERE Id=?''', (batchid, ))
            db.execute('''UPDATE Job SET State='prepared' WHERE Batch_id=?''', (batchid, ))
            db.commit()


def denetcdfize(db, criteria):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE State='netcdfized' AND ''' + criteria) 
    querylist = query.fetchall()
    for el in querylist:
        print('De-netcdfizing ' + str(el))
        batchid = el[0]
        batchdir = el[1]
        nc_files = glob.glob(batchdir + '/*.nc')
        for nc_file in nc_files:
            print('Removing ' + nc_file)
            os.remove(nc_file)
        tar_gz_files = glob.glob(batchdir + '/*.tar.gz')
        for tar_gz_file in tar_gz_files:
            with tarfile.open(tar_gz_file, 'r:gz') as tar:
                print('Untarring ' + tar_gz_file)
                tar.extractall(path=batchdir)
            os.remove(tar_gz_file)
        db.execute('''UPDATE Batch SET State='success' WHERE Id=?''', (batchid, ))
        db.execute('''UPDATE Job SET State='success' WHERE Batch_id=?''', (batchid, ))
        db.commit()


def netcdfize_el(db, el):
    print(el)
    batchid = el[0]
    batchdir = el[1]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        batch = QuaLiKizBatch.from_dir(batchdir)
    try: 
        batch.to_netcdf()
    except:
        e = sys.exc_info()[0]
        print(e)
        print(e.message)
        print('Error netcdfing, skipping forever')
        db.execute('''UPDATE Batch SET State='succes_nonetcdf' WHERE Id=?''', (batchid, ))
        db.commit()
    else:
        for i, run in enumerate(batch.runlist):
            print('Archiving ' + run.rundir)
            with tarfile.open(run.rundir + '.tar.gz', 'w:gz') as tar:
                tar.add(run.rundir, arcname=os.path.basename(run.rundir))
            if os.path.isfile(run.rundir + '.tar.gz'):
                shutil.rmtree(run.rundir)
                db.execute('''UPDATE Job SET State='archived'
                           WHERE Batch_id=? AND Job_id=?''',
                           (batchid, i))
                tries = 10
                for i in range(tries):
                    try:
                        db.commit()
                    except sqlite3.OperationalError:
                        time.sleep(1)
                else:
                    break

        db.execute('''UPDATE Batch SET State='netcdfized' WHERE Id=?''', (batchid, ))
        db.commit()

def netcdfize(db, limit):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE State='success' LIMIT ?''', (limit,))
    querylist = query.fetchall()
    for el in querylist:
        netcdfize_el(db, el)

def cancel(db, criteria):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE State='queued' AND ''' + criteria) 
    querylist = query.fetchall()
    for el in querylist:
        print(el)
        batchid = el[0]
        batchdir = el[1]
        jobnumber = el[2]
        output = sp.check_output(['scancel', str(jobnumber)])
        state = 'cancelled'
        db.execute('''UPDATE Batch SET State=?, Note='Manual cancel'
                   WHERE Id=?''',
                   (state, batchid))
        db.execute('''UPDATE Job SET State=?, Note='Manual cancel'
                   WHERE Batch_id=?''',
                   (state, batchid ))
        db.commit()


def hold(db, criteria):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE State='inputed' OR State='prepared' AND ''' + criteria) 
    querylist = query.fetchall()
    for el in querylist:
        print(el)
        batchid = el[0]
        batchdir = el[1]
        jobnumber = el[2]
        state = 'hold'
        db.execute('''UPDATE Batch SET State=?
                   WHERE Id=?''',
                   (state, batchid))
        db.execute('''UPDATE Job SET State=?
                   WHERE Batch_id=?''',
                   (state, batchid ))
        db.commit()

def tar(db, criteria, limit):
    query = db.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE ''' + criteria + ''' LIMIT ?''', (limit, )) 
    querylist = query.fetchall()
    for el in querylist:
        print(el)
        batchid = el[0]
        batchdir = el[1]
        jobnumber = el[2]
        with tarfile.open(batchdir + '.tar.gz', 'w:gz') as tar:
            tar.add(batchdir, arcname=os.path.basename(batchdir))

def trash(db):
    resp = input('Warning: This operation is destructive! Are you sure? [y/N]')
    if resp == 'Y' or resp == 'y':
        query = db.execute('''SELECT Id, Path from batch WHERE State='prepared' ''')
        querylist = query.fetchall()
        for el in querylist:
            print(el)
            batchid = el[0]
            batchdir = el[1]
            try:
                shutil.rmtree(batchdir)
            except FileNotFoundError:
                warn(dir + ' already gone')
            db.execute('''UPDATE Batch SET State='thrashed' WHERE Id=?''', (batchid, ))
            db.commit()

if __name__ == '__main__':
    queuelimit = 100
    jobdb = sqlite3.connect('jobdb.sqlite3')
    in_queue = waiting_jobs()
    numsubmit = max(0, queuelimit-in_queue)
    print(str(in_queue) + ' jobs in queue. Submitting ' + str(numsubmit))
    print('I can see ' + str(os.listdir()))
    #prepare_input(jobdb, numsubmit)
    #queue(jobdb, numsubmit)
    #cancel(jobdb, 'epsilon==0.33')
    #hold(jobdb, 'epsilon==0.33')
    #tar(jobdb, 'Ti_Te_rel==0.5', 2)
    #finished_check(jobdb)
    #netcdfize(jobdb, 1)
    print('I can see ' + str(os.listdir()))
    finished_check(jobdb)
    #archive(jobdb, 1)
    #denetcdfize(jobdb, 'epsilon==0.33')
    #clean_success(jobdb, 'epsilon==0.33')
    #trash(jobdb)
    jobdb.close()

    print('Script done')
    print(os.listdir())
    for i in range(10):
        try:
            os.remove(lockfile)
        except:
            print('Could not find logfile, try ' + str(i))
            time.sleep(10)
        else:
            break
    
    exit()
