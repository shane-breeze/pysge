import subprocess as sp

def run_command(cmd):
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    return p.communicate()
