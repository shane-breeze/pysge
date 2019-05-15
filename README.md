```
          pysge      +     +
    /\         /\     \   O \
   / \\  /\   /\\\     \_/|\_\
  /   \\/ \\ /  \\\       \\
       /   \/    \\\     \/'
      /    /      \\\     \\_
     /    /        \\\     -
```

## How to use

High-level interface use:

```
import pysge

tasks = [...] # list of dicts with keys ["task", "args", "kwargs"] run on each node as task(*args, **kwargs)

results = pysge.local_submit(tasks)
results = pysge.mp_submit(tasks, ncores=4)
results = pysge.sge_submit("name", "/tmp/pysge-temporaries", tasks=tasks, options="-q hep.q")
```
