import os
import time
import logging
from util.filemonitor import FileMonitor

logger = logging.getLogger('FileMonitor')

class MultiFileMonitor(object):
    def __init__(self, targets):
        self._monitors = {}
        for target in targets.keys():
            self._monitors[target] = FileMonitor(targets[target])

    def read(self):
        got_one = False
        while True:
            got_one = False
            for monitor in self._monitors.keys():
                data = self._monitors[monitor].read()
                if data is not None:
                    got_one = True
                    yield (monitor,data)
            if not got_one:
                time.sleep(0.1)
