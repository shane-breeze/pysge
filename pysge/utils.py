try:
    import subprocess32 as sp
except ModuleNotFoundError:
    import subprocess as sp
import shlex
import os

def run_command(cmd, env=os.environ):
    p = sp.run(shlex.split(cmd), stdout=sp.PIPE, stderr=sp.PIPE, env=env)
    return p.stdout, p.stderr
