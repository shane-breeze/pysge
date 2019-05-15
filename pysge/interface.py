from .area import WorkingArea
from .submitter import TaskSubmitter
from .monitor import JobMonitor

def submit(name, path, tasks=[], options="-q hep.q", dryrun=False):
    area = WorkingArea(path)
    submitter = TaskSubmitter(" ".join([f'-N {name}', options]))
    monitor = JobMonitor(submitter)

    results = {}
    area.create_areas(tasks)
    try:
        submitter.submit_tasks(area.task_paths, dryrun=dryrun)
        if not dryrun:
            results = monitor.monitor_jobs(sleep=10)
    except KeyboardInterrupt as e:
        submitter.killall()
    return results
