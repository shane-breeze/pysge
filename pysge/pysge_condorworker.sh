#!/bin/bash
ulimit -c 0
export PATH=$(echo $PATH | sed 's/^\/afs\/cern\.ch\/cms\/caf\/scripts:\/cvmfs\/cms\.cern\.ch\/common:\/cvmfs\/cms\.cern\.ch\/bin:\/bin://g')

echo $PATH
echo $PYTHONPATH
echo $LD_LIBRARY_PATH
echo $(which python)

pysge_worker.py
