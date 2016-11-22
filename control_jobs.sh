#!/bin/bash
SQS=$(squeue -hru karel --states=PENDING)
OLDIFS=$IFS
IFS=$'\n'
for line in $SQS
do
  IFS=$OLDIFS
  array=($line)
  jobid=${array[0]}
  echo Job $jobid
  scontrol hold $jobid
done
