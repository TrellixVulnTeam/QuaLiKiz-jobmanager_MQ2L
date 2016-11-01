import sqlite3
from qualikiz_tools.qualikiz_io.qualikizrun import QuaLiKizBatch, QuaLiKizRun

db = sqlite3.connect('jobdb.sqlite3')

queuelimit = 1
def prepare_input():
    query = db.execute('''SELECT Id, Path FROM batch WHERE State='prepared' LIMIT ?''', (str(queuelimit),))
    for el in query:
        batchid = el[0]
        dir = el[1]
        batch = QuaLiKizBatch.from_dir(dir)
        QuaLiKizBatch.generate_input
        #batch.generate_input()
        for run in batch.runlist:
            if not run.inputbinaries_exist():
                raise Exception('Generation of input binaries failed')
        db.execute('''UPDATE Batch SET State='inputed' WHERE Id=?''',
                   (batchid,))
        db.execute('''UPDATE Job SET State='inputed' WHERE Batch_id=?''',
                   (batchid,))
    db.commit()
    db.close()


def queue():
    query = db.execute('''SELECT Id, Path FROM batch WHERE State='inputed' LIMIT ?''', (str(queuelimit),))
    for el in query:
        batchid = el[0]
        dir = el[1]
        batch = QuaLiKizBatch.from_dir(dir)
        batch.queue_batch()

queue()
