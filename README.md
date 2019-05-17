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

## How to use

High-level interface use:

```
import pysge

tasks = [...] # list of dicts with keys ["task", "args", "kwargs"] run on each node as task(*args, **kwargs)

results = pysge.local_submit(tasks)
results = pysge.mp_submit(tasks, ncores=4)
results = pysge.sge_submit("name", "/tmp/pysge-temporaries", tasks=tasks, options="-q hep.q")
```

The return value is a list of results for each task, in order o tasks.

# How it works

For SGE batch system a working area is created and the functions + args + kwargs are pickled. A submitter then submits each pickled file to the batch using subprocess. A monitor checks the status of these jobs, waits until all are finished and returns the results.

For multiprocessing no working area is needed and the submitter uses multiprocessing.

For local no submitter is needed. It just loops over the tasks.
