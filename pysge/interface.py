import os
import dill
import gzip
import logging
from tqdm.auto import tqdm
from .area import WorkingArea
from .submitter import SGETaskSubmitter, MPTaskSubmitter
from .monitor import JobMonitor

logger = logging.getLogger(__name__)

def _validate_tasks(tasks):
    for task in tasks:
        if any(key not in task for key in ("task", "args", "kwargs")):
            return False
        if not callable(task["task"]):
            return False
    return True

def sge_submit(
    tasks, label, tmpdir, options="-q hep.q", dryrun=False, quiet=False,
    sleep=5, request_resubmission_options=True, return_files=False,
    dill_kw={"recurse": False},
):
    """
    Submit jobs to an SGE batch system. Return a list of the results of each
    job (i.e. the return values of the function calls)

    Parameters
    ----------
    tasks : list
        A list of dictrionaries with the keys: task, args and kwargs. Each
        element is run on a node as task(*args, **kwargs).

    label : str
        Label given to the qsub submission script through -N.

    tmpdir : str
        Path to temporary directory (doesn't have to exist) where pysge stores
        job infomation. Each call will have a unique identifier in the form
        tpd_YYYYMMDD_hhmmss_xxxxxxxx. Within this directory exists all tasks in
        separate directories with a dilled file, stdout and stderr for that
        particular job.

    options : str (default = "-q hep.q")
        Additional options to pass to the qsub command. Take care since the
        following options are already in use: -wd, -V, -e, -o and -t.

    dryrun : bool (default = False)
        Create directories and files but don't submit the jobs.

    quiet : bool (default = False)
        Don't print tqdm progress bars. Other prints are controlled by logging.

    sleep : float (default = 5)
        Minimum time between queries to the batch system.

    request_resubmission_options : bool (default = True)
        When a job fails the master process will expect an stdin from the user
        to alter the submission options (e.g. to increase walltime or memory
        requested). If False it will use the original options.

    return_files : bool (default = False)
        Instead of opening the output files and loading them into python, just
        send the paths to the output files and let the user deal with them.

    dill_kw : dict
        Kwargs to pass to dill.dump
    """
    if not _validate_tasks(tasks):
        logger.error(
            "Invalid tasks. Ensure tasks=[{'task': .., 'args': [..], "
            "'kwargs': {..}}, ...], where 'task' is callable."
        )
        return []
    area = WorkingArea(os.path.abspath(tmpdir))
    submitter = SGETaskSubmitter(" ".join(['-N {}'.format(label), options]))
    monitor = JobMonitor(submitter)

    results = []
    area.create_areas(tasks, quiet=quiet, dill_kw=dill_kw)
    try:
        submitter.submit_tasks(area.task_paths, dryrun=dryrun, quiet=quiet)
        if not dryrun:
            results = monitor.monitor_jobs(
                sleep=sleep, request_user_input=request_resubmission_options,
            )
    except KeyboardInterrupt as e:
        submitter.killall()

    if return_files:
        return results

    results_not_files = []
    for path in results:
        with gzip.open(path, 'rb') as f:
            results_not_files.append(dill.load(f))
    return results_not_files

def sge_submit_yield(
    tasks, label, tmpdir, options="-q hep.q", quiet=False, sleep=5,
    request_resubmission_options=True, dill_kw={"recurse": False},
):
    """
    Submit jobs to an SGE batch system. No monitoring is perfomed and the
    function returns an iterator over the results that yielding a list of the
    results. Jobs still running are given None in the list and the iterator
    terminates after all jobs are finished. This allows the user to perform
    some processing on partial results.

    Parameters
    ----------
    tasks : list
        A list of dictrionaries with the keys: task, args and kwargs. Each
        element is run on a node as task(*args, **kwargs).

    label : str
        Label given to the qsub submission script through -N.

    tmpdir : str
        Path to temporary directory (doesn't have to exist) where pysge stores
        job infomation. Each call will have a unique identifier in the form
        tpd_YYYYMMDD_hhmmss_xxxxxxxx. Within this directory exists all tasks in
        separate directories with a dilled file, stdout and stderr for that
        particular job.

    options : str (default = "-q hep.q")
        Additional options to pass to the qsub command. Take care since the
        following options are already in use: -wd, -V, -e, -o and -t.

    quiet : bool (default = False)
        Don't print tqdm progress bars. Other prints are controlled by logging.

    sleep : float (default = 5)
        Minimum time between queries to the batch system.

    request_resubmission_options : bool (default = True)
        When a job fails the master process will expect an stdin from the user
        to alter the submission options (e.g. to increase walltime or memory
        requested). If False it will use the original options.

    dill_kw : dict
        Kwargs to pass to dill.dump
    """

    if not _validate_tasks(tasks):
        logger.error(
            "Invalid tasks. Ensure tasks=[{'task': .., 'args': [..], "
            "'kwargs': {..}}, ...], where 'task' is callable."
        )
        return []
    area = WorkingArea(os.path.abspath(tmpdir))
    submitter = SGETaskSubmitter(" ".join(['-N {}'.format(label), options]))
    monitor = JobMonitor(submitter)

    area.create_areas(tasks, quiet=quiet, dill_kw=dill_kw)
    submitter.submit_tasks(area.task_paths, quiet=quiet)
    return monitor.request_jobs(
        sleep=sleep, request_user_input=request_resubmission_options,
    )

def sge_resume(
    label, tmpdir, options="-q hep.q", quiet=False, sleep=5,
    request_resubmission_options=True, return_files=False,
):
    """
    Resubmit jobs based on the temporary directory (with the tpd_*
    subdirectory). The original submission may terminate due to unforseen
    reasons. This will continue where everything left off.

    Parameters
    ----------
    label : str
        Label given to the qsub submission script through -N.

    tmpdir : str
        Path to temporary directory created by an sge_submit or similar.

    options : str (default = "-q hep.q")
        Additional options to pass to the qsub command. Take care since the
        following options are already in use: -wd, -V, -e, -o and -t.

    quiet : bool (default = False)
        Don't print tqdm progress bars. Other prints are controlled by logging.

    sleep : float (default = 5)
        Minimum time between queries to the batch system.

    request_resubmission_options : bool (default = True)
        When a job fails the master process will expect an stdin from the user
        to alter the submission options (e.g. to increase walltime or memory
        requested). If False it will use the original options.

    return_files : bool (default = False)
        Instead of opening the output files and loading them into python, just
        send the paths to the output files and let the user deal with them.
    """
    area = WorkingArea(os.path.abspath(tmpdir), resume=True)
    submitter = SGETaskSubmitter(" ".join(['-N {}'.format(label), options]))
    for idx in range(len(area.task_paths)):
        submitter.jobid_tasks['{}'.format(idx)] = area.task_paths[idx]
    monitor = JobMonitor(submitter)

    results = []
    try:
        results = monitor.monitor_jobs(
            sleep=sleep, request_user_input=request_resubmission_options,
        )
    except KeyboardInterrupt as e:
        submitter.killall()

    if return_files:
        return results

    results_not_files = []
    for path in results:
        with gzip.open(path, 'rb') as f:
            results_not_files.append(dill.load(f))
    return results_not_files

def mp_submit(tasks, ncores=4, quiet=False):
    """
    Submit multiprocessing jobs. Each task spawns a new child process which is
    run on an available core. In multiprocessing mode the functions are
    pickled and sent to each core, hence this is good at testing small scale
    local submission before submitting to the batch. Return a list of the
    results of each job (i.e. the return values of the function calls)

    Parameters
    ----------
    tasks : list
        A list of dictrionaries with the keys: task, args and kwargs. Each
        element is run on a node as task(*args, **kwargs).

    ncores : int
        The number of cores to run on.

    quiet : bool (default = False)
        Don't print tqdm progress bars. Other prints are controlled by logging.
    """
    if not _validate_tasks(tasks):
        logger.error(
            "Invalid tasks. Ensure tasks=[{'task': .., 'args': [..], "
            "'kwargs': {..}}, ...], where 'task' is callable."
        )
        return []
    submitter = MPTaskSubmitter()
    return submitter.submit_tasks(tasks, ncores=ncores, quiet=quiet)

def local_submit(tasks, quiet=False):
    """
    Submit local jobs. Mainly for testing purposes.

    Parameters
    ----------
    tasks : list
        A list of dictrionaries with the keys: task, args and kwargs. Each
        element is run on a node as task(*args, **kwargs).

    quiet : bool (default = False)
        Don't print tqdm progress bars. Other prints are controlled by logging.
    """
    if not _validate_tasks(tasks):
        logger.error(
            "Invalid tasks. Ensure tasks=[{'task': .., 'args': [..], "
            "'kwargs': {..}}, ...], where 'task' is callable."
        )
        return []

    results = []
    pbar = tqdm(total=len(tasks), desc="Finished", disable=quiet, ncols=80)

    try:
        for t in tasks:
            results.append(t["task"](*t["args"], **t["kwargs"]))
            pbar.update()
    except KeyboardInterrupt:
        results = []

    pbar.close()
    return results
