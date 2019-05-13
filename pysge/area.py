import os
import datetime
import tempfile
import gzip
import pickle
from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)

class WorkingArea(object):
    def __init__(self, path):
        self.task_paths = None
        prefix = 'tpd_{:%Y%m%d_%H%M%S}_'.format(datetime.datetime.now())
        self.path = tempfile.mkdtemp(prefix=prefix, dir=os.path.abspath(path))
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def create_areas(self, tasks):
        task_paths = []
        logger.info('Creating paths in {}'.format(self.path))
        for idx, task in tqdm(enumerate(tasks), total=len(tasks)):
            package_name = f'task_{idx:05d}'
            path = os.path.join(self.path, package_name)
            if not os.path.exists(path):
                os.makedirs(path)
            file_path = os.path.join(os.path.join(path, "task.p.gz"))
            with gzip.open(file_path, 'wb') as f:
                pickle.dump(task, f)
            task_paths.append(path)
        self.task_paths = task_paths
