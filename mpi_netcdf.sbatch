#!/bin/bash -l
#SBATCH -p debug
#SBATCH -N 22
#SBATCH -t 00:30:00
#SBATCH -L SCRATCH     #note: specify license need for the file systems your job needs, such as SCRATCH,project

srun -n 44 -c 24 -K0 python mpi_netcdf.py
