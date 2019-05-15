import subprocess as sp
import shlex

def run_command(cmd):
    p = sp.Popen(shlex.split(cmd), stdout=sp.PIPE, stderr=sp.PIPE)
    return p.communicate()
