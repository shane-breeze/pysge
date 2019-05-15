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

The return value is a dictionary of taskid's to the return value of the function being called (probably should just be a list corresponding to the input list).
