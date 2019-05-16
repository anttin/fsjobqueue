import datetime
import os
import re
import threading
import time

from fsjobqueue import job
from fsjobqueue.job import Job
from threading import Thread
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class JobQueueWorker(object):
  """
  This class works as a filesystem-backed-up queue system for jobs.

  Params:
    - path: the directory where the queue resides on the file system
    - job_life_minutes: how many mitues the job will be kept in the queue before eventually removed (regardless of status)
  """
  def __init__(self, path, job_life_minutes=240):
    self.filename_regex = re.compile(job.FILENAME_TEMPLATE.format(job_name='({})'.format(job.JOB_NAME_FORMAT)))

    self.job_life_minutes = job_life_minutes
    self.housekeeping_interval_sec = 300

    self.path = path.rstrip('/')
    self.jobs = self._read_jobs_from_filesystem()

    self.callback_registry = {}

    self._observer = Observer()
    self._handler  = JobQueueWorker.EventHandler(self)
    self._observer.schedule(self._handler, self.path)

    self._wthread        = Thread(target=self._th_run)
    self._wthread.name   = 'FilesystemWatchdog'
    self._wthread.daemon = True
    self._stop_running   = False

    self._wthread.start()
    self.running = True

    self._hthread        = Thread(target=self._th_housekeeping)
    self._hthread.name   = 'Housekeeping'
    self._hthread.daemon = True
    self._hthread.start()


  def _th_run(self):
    self._observer.start()
    try:
      while self._stop_running == False:
        time.sleep(5)
    except:
        self._observer.stop()

    self.running = False
    self._observer.join()


  def _th_housekeeping(self):
    last_run = datetime.datetime.now()
    a = []
    while self._stop_running == False:
      now = datetime.datetime.now()

      if (now - last_run).seconds > self.housekeeping_interval_sec:
        for k, v in self.jobs.items():
          if now > v.expires:
            a.append(k)

        if len(a) > 0:
          for jobname in a:
            self.remove_job(jobname, mustexist=False)
          a = []

      time.sleep(5)


  def _read_jobs_from_filesystem(self):
    result   = {}
    filelist = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
    filelist.sort()
 
    for filepath in filelist:
      fname = os.path.basename(filepath)
      if not self.filename_regex.match(fname):
        continue
      j = Job.read(self.path, fname)
      result[j.name] = j

    return result


  def wait(self):
    while self.running == True:
      time.sleep(1)


  def read_job(self, filename):
    """
    Read a job from the filesystem
    """
    j = Job.read(self.path, filename)
    self.jobs[j.name] = j
    return j


  def add_job(self, data):
    """
    Add a new job to queue and write the jobfile to the filesystem
    """
    exp = datetime.datetime.now()+datetime.timedelta(minutes=self.job_life_minutes)
    j = Job(self.path, data, expires=exp)
    self.jobs[j.name] = j
    self.jobs[j.name].write()
    return self.jobs[j.name]


  def get_job(self, jobname):
    """
    Fetch an existing job from the queue based on the jobname
    """
    if not jobname in self.jobs:
      raise JobNotFoundException('Job {} not found'.format(jobname))
    return self.jobs[jobname]


  def remove_job(self, jobname, mustexist=True):
    """
    Remove job from job list and filesystem. This is used when a job needs to be removed.
    """
    if mustexist == True and not jobname in self.jobs:
      raise JobNotFoundException('Job {} not found'.format(jobname))
    if os.path.isfile(self.jobs[jobname].filepath):
      os.remove(self.jobs[jobname].filepath)
    self.clean_job(jobname)


  def clean_job(self, jobname):
    """
    Remove job from job list without touching the filesystem. This is typically used when the file is removed from the file system.
    """
    if jobname in self.jobs:
      del self.jobs[jobname]


  def register_callback(self, event, callback):
    """
    Register a function for callback in case of event of type <event> is encountered.
    Callback function must take three parameters: string(event_type), object(Job) and string(filename)

    For example:

    def react_to_create(event_type, job_object, filename):
      print("Reacting to {} by job {} in file {}".format(event_type, job_object.name, filename))

    queue = jobqueue.JobQueueWorker('/tmp/queue')
    queue.register_callback('created', react_to_create)

    Please note that for some events, the job_object or filename may be None.
    """
    if event not in self.callback_registry:
      self.callback_registry[event] = []

    if callback not in self.callback_registry[event]:
      self.callback_registry[event].append(callback)


  def unregister_callback(self, event, callback):
    """
    Unregister previously registered callback.
    """
    if event in self.callback_registry and callback in self.callback_registry[event]:
      self.callback_registry[event].remove(callback)  


  class EventHandler(FileSystemEventHandler):
    def __init__(self, queueworker):
      super().__init__()
      self.queueworker = queueworker


    def _do_callbacks(self, event, job_obj=None, filename=None):
      if event in self.queueworker.callback_registry:
        for callback_func in self.queueworker.callback_registry[event]:
          callback_func(event, job_obj, filename)


    def on_any_event(self, event):
      if event.is_directory:
        return None

      fname = os.path.basename(event.src_path)

      if not self.queueworker.filename_regex.match(fname):
        # just ignore filenames of wrong format
        return

      if event.event_type in ['created', 'modified']:
        job_obj = self.queueworker.read_job(fname)
        self._do_callbacks(event.event_type, job_obj=job_obj, filename=fname)

      elif event.event_type == 'moved':
        job_obj = self.queueworker.read_job(os.path.basename(event.dst_path))
        self._do_callbacks(event.event_type, job_obj=job_obj, filename=os.path.basename(event.dst_path))

      elif event.event_type == 'deleted':
        r = self.queueworker.filename_regex.match(fname)
        self.queueworker.clean_job(r.group(1))
        self._do_callbacks(event.event_type, filename=fname)





###############################################################################


class JobNotFoundException(Exception):
  def __init__(self, message):
    super().__init__(message)
