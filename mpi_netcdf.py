"""
Copyright Dutch Institute for Fundamental Energy Research (2016)
Contributors: Karel van de Plassche (karelvandeplassche@gmail.com)
License: CeCILL v2.1

Launch this script with the supplied mpi_netcdf.sbatch to netCDFize finished runs
in parallel.
"""
from mpi4py import MPI
import sqlite3
from launch_run import netcdfize_el
import time
import traceback

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

jobdb = sqlite3.connect('jobdb.sqlite3', timeout=300, cached_statements=2000)
if rank == 0:
    limit = size
    query = jobdb.execute('''SELECT Id, Path, Jobnumber FROM batch
                       WHERE State='success' LIMIT ?''', (limit,))
    querylist = query.fetchall()
    starttime = time.time()
else:
    querylist = None
    starttime = None
querylist = comm.bcast(querylist, root=0)
starttime = comm.bcast(starttime, root=0)
print('processor ' + str(MPI.Get_processor_name()) + ' with rank ' + str(rank) + ' is netcdfizing')
try:
    netcdfize_el(jobdb, querylist[rank])
except Exception as err:
    traceback.print_tb(err.__traceback__)
    print('processor ' + str(MPI.Get_processor_name()) + ' with rank ' + str(rank) + ' raised an exception after ' + str(time.time()-starttime))
    raise
print('processor ' + str(MPI.Get_processor_name()) + ' with rank ' + str(rank) + ' is done after ' + str(time.time()-starttime))

exit()
