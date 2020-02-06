import os
import logging
import gzip
import time
import copy
import dill
from tqdm.auto import tqdm
from .utils import run_command
logger = logging.getLogger(__name__)

SGE_JOBSTATUS = {
    1: "Running",   "Running": 1,
    2: "Pending",   "Pending": 2,
    3: "Suspended", "Suspended": 3,
    4: "Error",     "Error": 4,
    5: "Deleted",   "Deleted": 5,
    6: "Finished",  "Finished": 6,
}

CONDOR_JOBSTATUS = {
    0: "Unexpanded", "Unexpanded": 0,
    1: "Pending",    "Pending": 1,
    2: "Running",    "Running": 2,
    3: "Deleted",    "Deleted": 3,
    4: "Finished",   "Finished": 4,
    5: "Held",       "Held": 5,
    6: "Error",      "Error": 6,
}

# https://gist.github.com/cmaureir/4fa2d34bc9a1bd194af1
SGE_JOBSTATE_CODES = {
    # Running
    "r": 1,
    "t": 1,
    "Rr": 1,
    "Rt": 1,
    "hr": 1,

    # Pending
    "qw": 2,
    "hqw": 2,
    "hRwq": 2,

    # Suspended
    "s": 3, "ts": 3,
    "S": 3, "tS": 3,
    "T": 3, "tT": 3,
    "Rs": 3, "Rts":3, "RS":3, "RtS":3, "RT":3, "RtT": 3,

    # Error
    "Eqw": 4, "Ehqw": 4, "EhRqw": 4,

    # Deleted
    "dr": 5, "dt": 5, "dRr": 5, "ds": 5, "dS": 5, "dT": 5, "dRs": 5, "dRS": 5, "dRT": 5,
}

class JobMonitor(object):
    def __init__(self, submitter):
        self.submitter = submitter
        self.jobstatus = {}
        self.jobstate_codes = {}

    def monitor_jobs(self, sleep=5, request_user_input=True):
        jobid_tasks = self.submitter.jobid_tasks
        ntotal = len(jobid_tasks)

        pbar_run = tqdm(total=ntotal, desc="Running ")
        pbar_fin = tqdm(total=ntotal, desc="Finished")

        for running, results in self.return_finished_jobs(request_user_input=request_user_input):
            pbar_run.n = len(running)
            pbar_fin.n = len([r for r  in results if r is not None])
            pbar_run.refresh()
            pbar_fin.refresh()
            time.sleep(sleep)

        pbar_run.close()
        pbar_fin.close()
        print("")
        return results

    def request_jobs(self, sleep=5, request_user_input=True):
        jobid_tasks = self.submitter.jobid_tasks
        ntotal = len(jobid_tasks)

        pbar_run = tqdm(total=ntotal, desc="Running ")
        pbar_fin = tqdm(total=ntotal, desc="Finished")
        try:
            for running, results in self.return_finished_jobs(request_user_input=request_user_input):
                pbar_run.n = len(running)
                pbar_fin.n = len([r for r  in results if r is not None])
                pbar_run.refresh()
                pbar_fin.refresh()
                time.sleep(sleep)
                yield results
        except KeyboardInterrupt as e:
            self.submitter.killall()

        pbar_run.close()
        pbar_fin.close()
        print("")
        yield results

    def return_finished_jobs(self, request_user_input=True):
        jobid_tasks = self.submitter.jobid_tasks
        ntotal = len(jobid_tasks)
        nremaining = ntotal

        finished, results = [], [None]*ntotal

        while nremaining>0:
            job_statuses = self.query_jobs()
            all_queried_jobs = []
            for state, queried_jobs in job_statuses.items():
                if state not in [self.jobstatus["Finished"]]:
                    all_queried_jobs.extend(queried_jobs)

            jobs_not_queried = {
                jobid: task
                for jobid, task in self.submitter.jobid_tasks.items()
                if jobid not in all_queried_jobs and jobid not in finished
            }
            finished.extend(self.check_jobs(
                jobs_not_queried, results,
                request_user_input=request_user_input,
            ))

            nremaining = ntotal - len(finished)
            yield job_statuses.get(self.jobstatus["Running"], {}), results

        # all jobs finished - final loop
        yield {}, results

    def check_jobs(self, jobid_tasks, results, request_user_input=True):
        finished = []
        for jobid, task in jobid_tasks.items():
            pos = int(os.path.basename(task).split("_")[-1])
            try:
                with gzip.open(os.path.join(task, "result.p.gz"), 'rb') as f:
                    dill.load(f)
                results[pos] = os.path.join(task, "result.p.gz")
                finished.append(jobid)
            except (IOError, EOFError, dill.UnpicklingError) as e:
                logger.info('Resubmitting {}: {}'.format(jobid, task))
                self.submitter.submit_tasks(
                    [task], start=pos, request_user_input=request_user_input,
                )
                self.submitter.jobid_tasks.pop(jobid)

        return finished

    def query_jobs(self):
        raise NotImplementedError(
            "JobMonitor.query_jobs must be implemented in inherited class"
        )

class SGEJobMonitor(JobMonitor):
    def __init__(self, submitter):
        JobMonitor.__init__(self, submitter)
        self.jobstatus = SGE_JOBSTATUS
        self.jobstate_codes = SGE_JOBSTATE_CODES

    def query_jobs(self):
        job_status = {}
        out, err = run_command("qstat -g d")

        for l in out.splitlines():
            if l.startswith("job-ID") or l.startswith("-----"):
                continue
            ws = l.split()
            jobid = ws[0]
            taskid = int(ws[-1])

            if not '{}.{}'.format(jobid, taskid) in self.submitter.jobid_tasks.keys():
                continue

            state = self.jobstate_codes[ws[4]]
            if state not in job_status:
                job_status[state] = []
            job_status[state].append('{}.{}'.format(jobid, taskid))

        return job_status

class CondorJobMonitor(JobMonitor):
    def __init__(self, submitter):
        JobMonitor.__init__(self, submitter)
        self.jobstatus = CONDOR_JOBSTATUS

    def query_jobs(self):
        job_status = {}

        for job in self.submitter.schedd.xquery(
            projection=["ClusterId", "ProcId", "JobStatus"],
        ):
            jobid = job["ClusterId"]
            taskid = job["ProcId"]
            state = job["JobStatus"]
            if not '{}.{}'.format(jobid, taskid) in self.submitter.jobid_tasks.keys():
                continue

            if state not in job_status:
                job_status[state] = []
            job_status[state].append('{}.{}'.format(jobid, taskid))

        return job_status
