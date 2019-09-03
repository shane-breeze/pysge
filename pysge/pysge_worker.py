#!/usr/bin/env python
import lz4.frame
import dill
import os

def main():
    cwd = os.getcwd()
    with lz4.frame.open("task.p.lz4", 'rb') as f:
        task = dill.load(f)

    print("Task = {}\n\nargs = {}\n\nkwargs = {}\n".format(
        task["task"], task["args"], task["kwargs"],
    ))
    result = task["task"](*task["args"], **task["kwargs"])

    # Just incase the user wants to change directory within the task
    os.chdir(cwd)

    with lz4.frame.open("result.p.lz4", 'wb') as f:
        dill.dump(result, f)

if __name__ == "__main__":
    main()
