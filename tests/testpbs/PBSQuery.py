# dummy PBSQuery module

import os
fn = 'master3_dump_20130316.py'



class PBSQuery(object):
    def __init__(self, *args, **kwargs):
        dump = os.path.join(os.path.dirname(__file__), fn)

        self._data = {}
        execfile(dump, self._data)


    def getnodes(self, *args, **kwargs):
        return self._data['nodes']

    def getqueues(self, *args, **kwargs):
        return self._data['queues']

    def getjobs(self, *args, **kwargs):
        return self._data['jobs']

