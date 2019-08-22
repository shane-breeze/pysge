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

results = pysge.local_submit(tasks)
results = pysge.mp_submit(tasks, ncores=3)
results = pysge.sge_submit(tasks, "name", "/tmp/pysge-temporaries", options="-q hep.q")
```

The return value is a list of results for each task, in order of tasks.

# How it works

For SGE batch system a working area is created and the functions + args + kwargs are dilled. A submitter then submits each dilled file to the batch using subprocess. A monitor checks the status of these jobs, waits until all are finished and returns the results.

For multiprocessing no working area is needed and the submitter uses multiprocessing.

For local no submitter is needed. It just loops over the tasks.
