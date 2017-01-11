# QuaLiKiz-jobmanager

*A collection of scripts for managing many QuaLiKiz runs on Edison*

## Purpose

This is a collection of Python and bash scripts that can be used to
manage many QuaLiKiz runs on the Edsion supercomputer. Qualikiz is a
quasi-linear gyrokinetic code and can be found
[on GitHub](https://github.com/QuaLiKiz-group/QuaLiKiz).

Developing these scripts further is not planned, so this repository is
more for archiving purposes. The 
[QuaLiKiz Neural Network](https://github.com/QuaLiKiz-group/QuaLiKiz-NeuralNetwork)
training set was generated using these tools.


## Usage
A workflow example is given below:

1. Adjust the `parameters.json` to your liking. This will be used as base for
    all the runs.

2. Adjust the `scan_parameters.csv` to set up the hyperrectangle that will be
    scanned over.

3. Generate the skeleton for all the runs and create the job database.

        python initialize_megadb.py

4. Set up a folder tree to archive files in the tape storage system (hsi)

        python create_netcdf_foldertree.py

5. Periodically manage the jobs. For example, create a cron job that runs:

        python launch_run.py
