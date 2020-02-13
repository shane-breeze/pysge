import os
import datetime
import tempfile
import gzip
import glob
from tqdm.auto import tqdm
import dill
import logging
logger = logging.getLogger(__name__)

class WorkingArea(object):
    def __init__(self, path, resume=False):
        self.task_paths = None

        if resume:
            self.path = path
            self.get_areas()
        else:
            prefix = 'tpd_{:%Y%m%d_%H%M%S}_'.format(datetime.datetime.now())

            if not os.path.exists(path):
                os.makedirs(path)
            self.path = tempfile.mkdtemp(prefix=prefix, dir=os.path.abspath(path))
            if not os.path.exists(self.path):
                os.makedirs(self.path)

    def create_areas(self, tasks, quiet=False, dill_kw={"recurse": False}):
        task_paths = []
        logger.info('Creating paths in {}'.format(self.path))
        for idx, task in tqdm(
            enumerate(tasks), total=len(tasks), disable=quiet, ncols=80,
        ):
            package_name = 'task_{:05d}'.format(idx)
            path = os.path.join(self.path, package_name)
            if not os.path.exists(path):
                os.makedirs(path)
            file_path = os.path.join(os.path.join(path, "task.p.gz"))
            with gzip.open(file_path, 'wb') as f:
                dill.dump(task, f, **dill_kw)
            task_paths.append(path)
        self.task_paths = task_paths

    def get_areas(self):
        self.task_paths = list(glob.glob(os.path.join(self.path, "task_*")))
