```
          pysge      +     +
    /\         /\     \   O \
   / \\  /\   /\\\     \_/|\_\
  /   \\/ \\ /  \\\       \\
       /   \/    \\\     \/'
      /    /      \\\     \\_
     /    /        \\\     -
```

Call python functions on (IC) SGE batch cluster - with tqdm progress bars.
Designed to work inside a jupyter notebook.

## How to use

[Docs](https://shane-breeze.github.io/pysge/).

High-level interface use:

```
import pysge

def myfunc(a, b):
    return a + b

tasks = [
    {"task": myfunc, "args": (1, 2), "kwargs": {}},
    {"task": myfunc, "args": (3, 4), "kwargs": {}},
    {"task": myfunc, "args": (5, 6), "kwargs": {}},
]

results = pysge.local_submit(tasks) # single process for loop over tasks - tasks stored in memory
results = pysge.mp_submit(tasks, ncores=3) # multiple processes with a queue for the tasks - tasks stored in memory
results = pysge.sge_submit(tasks, "name", "/tmp/pysge-temporaries", options="-q hep.q") # SGE batch pool queue for the tasks - tasks stored on disk
# WIP: results = pysge.condor_submit(tasks, "name", "/tmp/pysge-temporaries") # Condor batch pool queue for the tasks - tasks stored on disk
print(results) # [3, 7, 11]
```

The return value is a list of results for each task, in order of tasks.

# How it works

For SGE batch system a working area is created and the functions + args + kwargs are dilled. A submitter then submits each dilled file to the batch using subprocess. A monitor checks the status of these jobs, waits until all are finished and returns the results.

For multiprocessing no working area is needed and the submitter uses multiprocessing.

For local no submitter is needed. It just loops over the tasks.
