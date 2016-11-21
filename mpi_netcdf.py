from mpi4py import MPI
import sqlite3
from launch_run import netcdfize_el

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

jobdb = sqlite3.connect('jobdb.sqlite3')
if rank == 0:
    limit = size
    query = jobdb.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE State='success' LIMIT ?''', (limit,))
    querylist = query.fetchall()
else:
    querylist = None
querylist = comm.bcast(querylist, root=0)
print('processor ' + str(MPI.Get_processor_name()) + ' with rank ' + str(rank) + ' is netcdfizing')
netcdfize_el(jobdb, querylist[rank])
print('processor ' + str(MPI.Get_processor_name()) + ' with rank ' + str(rank) + ' is done')


exit()
