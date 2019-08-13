#!/usr/bin/env python
import lz4.frame

try:
    import cPickle as pickle
except ImportError:
    import pickle

def main():
    with lz4.frame.open("task.p.lz4", 'rb') as f:
        task = pickle.load(f)

    print("Task = {}\n\nargs = {}\n\nkwargs = {}\n".format(
        task["task"], task["args"], task["kwargs"],
    ))
    result = task["task"](*task["args"], **task["kwargs"])

    with lz4.frame.open("result.p.lz4", 'wb') as f:
        pickle.dump(result, f)

if __name__ == "__main__":
    main()
