#!/usr/bin/env python
import gzip
import dill
import os

def main():
    cwd = os.getcwd()
    with gzip.open("task.p.gz", 'rb') as f:
        task = dill.load(f)

    print("Task = {}\n\nargs = {}\n\nkwargs = {}\n".format(
        task["task"], task["args"], task["kwargs"],
    ))
    result = task["task"](*task["args"], **task["kwargs"])

    # Just incase the user wants to change directory within the task
    os.chdir(cwd)

    with gzip.open("result.p.gz", 'wb') as f:
        dill.dump(result, f)

if __name__ == "__main__":
    main()
