import os
import re
import logging

from .utils import run_command

logger = logging.getLogger(__name__)

class TaskSubmitter(object):
    submit_command = 'qsub -cwd -V -e /dev/null -o /dev/null -t 1-{njobs}:1 {job_opts} $(which {executable})'
    regex_submit = re.compile('Your job-array (?P<jobid>[0-9]+)\.1-[0-9]+:1 \("pysge_worker\.sh"\) has been submitted')
    def __init__(self, job_options):
        self.job_options = job_options
        self.jobid_tasks = {}

    def submit_tasks(self, tasks):
        if tasks is None or len(tasks) <= 0:
            return

        curdir = os.getcwd()
        njobs = len(tasks)
        os.chdir(os.path.dirname(tasks[0]))
        cmd = self.submit_command.format(
            executable="pysge_worker.sh", njobs=njobs,
            job_opts=self.job_options,
        )
        out, err = run_command(cmd)
        os.chdir(curdir)

        match = self.regex_submit.search(out.decode("utf-8"))
        jobid = int(match.group("jobid"))
        logger.info(f'Submitted {jobid}.1-{njobs}:1')

        for aid in range(njobs):
            arrayid = aid+1
            self.jobid_tasks[f'{jobid}.{arrayid}'] = tasks[aid]
