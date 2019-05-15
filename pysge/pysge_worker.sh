#!/bin/bash
ulimit -c 0
taskid=$((SGE_TASK_ID - 1))
path=task_$(printf "%05d" ${taskid})
cd $path
pysge_worker.py > stdout.txt 2> stderr.txt
