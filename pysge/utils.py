try:
    import subprocess32 as sp
except ModuleNotFoundError:
    import subprocess as sp
import shlex

def run_command(cmd, input_=None):
    args = (shlex.split(cmd),)
    kwargs = {"stdout": sp.PIPE, "stderr": sp.PIPE, "encoding": 'utf-8'}
    if input_ is not None:
        kwargs["input"] = input_

    p = sp.run(*args, **kwargs)
    return p.stdout, p.stderr
