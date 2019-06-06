import datetime
import dateutil.parser
import json
import random
import re
import string

from enum import Enum

FILENAME_TEMPLATE = '^\d{{8}}_\d{{6}}_\d{{6}}_{job_name}.json$'
JOB_NAME_FORMAT   = '[a-zA-Z0-9]+'


class Job(object):
  class Status(Enum):
    QUEUED     = 0   # job has been queued but not yet processing
    PROCESSING = 1   # job is currently being processed
    SUCCESSFUL = 2   # job is succesfully finished 
    FAILED     = -1  # job has failed


  def __init__(self, queue_path, data={}, name=None, filename=None, expires=None, status=Status.QUEUED, message=None):
    self._queue_path = queue_path # path to the directory where the job files are stored
    self.data        = data
    self.name        = Job.get_name() if name is None else name
    self.filename    = Job.get_new_filename(self.name) if filename is None else filename
    self.expires     = datetime.datetime.now()+datetime.timedelta(minutes=240) if expires is None else expires
    self.status      = status
    self.message     = message


  @property
  def filepath(self):
    """
    filepath == full path including filename
    """
    return '{}/{}'.format(self._queue_path.rstrip('/'), self.filename)


  def serialize(self):
    r = {
      'name':     self.name,
      'filename': self.filename,
      'expires':  self.expires.isoformat(),
      'data':     self.data,
      'status':   self.status.value,
      'message':  self.message
    }
    return json.dumps(r, indent=4, sort_keys=True, separators=(',', ': '))


  def write(self):
    """
    Serializes itself and writes job file to filesystem.

    User must handle exceptions
    """
    with open(self.filepath, 'w', encoding='utf8') as file:
      file.write(self.serialize()) 


  @staticmethod
  def deserialize(queue_path, json_text):
    def check_keys(required_keys, keys):
      for key in required_keys:
        if key not in keys:
          return False
      return True
      
    j = json.loads(json_text)
    if not check_keys(['name', 'filename', 'expires', 'data', 'status'], j.keys()):
      raise Exception('Error while deserializing, required keys missing')

    return Job(
      queue_path, 
      name=j['name'], 
      data=j['data'], 
      filename=j['filename'], 
      expires=dateutil.parser.parse(j['expires']), 
      status=Job.Status(int(j['status'])),
      message=j['message']
    )


  @staticmethod
  def read(queue_path, filename):
    """
    Reads jobfile from filesystem, deserializes data and returns Job-object.

    User must handle exceptions
    """
    with open('{}/{}'.format(queue_path.rstrip('/'), filename), encoding='utf8') as file:
      data = file.read()
    return Job.deserialize(queue_path, data)


  @staticmethod
  def get_name(name_length=12):
    """
    Returns random name (string) for job
    """
    letters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(letters) for i in range(name_length))


  @staticmethod
  def get_new_filename(name):
    """
    Returns filename for job
    """
    return '{}_{}.json'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f'), name)
