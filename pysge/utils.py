try:
    import subprocess32 as sp
except ImportError:
    import subprocess as sp
import shlex

def run_command(cmd, input_=None):
    args = (shlex.split(cmd),)
    kwargs = {"stdout": sp.PIPE, "stderr": sp.PIPE}
    if input_ is not None:
        kwargs["input"] = input_

    p = sp.run(*args, **kwargs)
    return p.stdout.decode('utf-8'), p.stderr.decode('utf-8')
