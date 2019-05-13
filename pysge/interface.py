from .area import WorkingArea
from .submitter import TaskSubmitter
from .monitor import JobMonitor

def submit(name, path, tasks=[], options="-q hep.q"):
    area = WorkingArea(path)
    submitter = TaskSubmitter(" ".join([f'-N {name}', options]))
    monitor = JobMonitor(submitter)

    area.create_areas(tasks)
    submitter.submit_tasks(area.task_paths)
    return monitor.monitor_jobs(sleep=10)
