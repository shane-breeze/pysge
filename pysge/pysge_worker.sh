#!/bin/bash
ulimit -c 0
taskid=$((SGE_TASK_ID - 1))
path=task_$(printf "%05d" ${taskid})
cd $path
{ time pysge_worker.py ; } > $PWD/stdout.txt 2> $PWD/stderr.txt
