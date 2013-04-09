# -*- coding: utf-8 -*-
import collections
import logging
import glob
import os
import ujson
#from exceptions import TaskFactoryError, TaskNotIterableError

class JobFactory(object):
    def __init__(self, **kwargs):
        self.logger = logging.getLogger('JobFactory')
        self.__dict__.update(kwargs)

    def __iter__(self):
        self._validate()

        left, right = self.task_range
        current = left + (self.offset - 1)
        job_id = 0
        while left < right:
            self.job_id = job_id
            self.job_range = [left, current]

            yield self._get_job()

            left += self.offset
            current += self.offset
            job_id += 1

            if current > right:
                current = right

        raise StopIteration

    def _validate(self):
        if not isinstance(self.task_range, collections.Iterable):
            raise TaskNotIterableError('The task is not iterable.')
        if len(self.task_range) > 2:
            raise TaskNotIterableError('The task interval must have 2 elements.')
        if self.task_range[0] > self.task_range[1]:
            raise TaskNotIterableError('The left side of interval is higher that right side.')

    def _get_job(self):
        job = dict(self.__dict__.items())
        job.pop('logger') # Needed to queue store the job, picker error if not poped

        return Job(**job)

class Job(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(dict(self.__dict__.items()))


class TaskDiskFactory(object):
    def __init__(self):
        self.logger = logging.getLogger('TaskDiskFactory')
        self.pool = './pool/'

    def __iter__(self):
        for tfile in glob.iglob(self.pool + '*.json'):
            yield TaskDisk(tfile)()
            self.logger.debug("Task {0} readed!.".format(tfile))

class TaskDisk(object):
    def __init__(self, tfile):
        self._tfile = tfile

    def _read(self):
        with open(self._tfile) as tfile:
            task = ujson.loads(tfile.read())
            task['parent'] = self._tfile
            return task

    def __del__(self):
        os.rename(self._tfile, self._tfile + '.queue')

    def __call__(self):
        return self._read()

class TaskDatabaseFactory(object):
    pass
