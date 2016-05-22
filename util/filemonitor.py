import os
import time
import logging


logger = logging.getLogger('FileMonitor')


class FileMonitor(object):
    def __init__(self, target):
        self._target = target
        self._position = None
        self._size = None
        self._inode = None
        self._fd = None
        self._read_from_beginning = False

    def open(self):
        self._fd = open(self._target, 'r')
        st_results = os.stat(self._target)
        self._size = st_results[6]
        self._inode = st_results.st_ino
        if not self._read_from_beginning:
            self._fd.seek(self._size)
        self._position = self._fd.tell()

    def reopen(self):
        if self._fd is not None:
            self._fd.close()
        self.open()

    def check_file_sanity(self):
        if self._fd is None:
            self.open()
            return
        st_results = os.stat(self._target)
        if st_results.st_ino != self._inode:
            logger.info('file rotated, reopening...')
            self._read_from_beginning = True
            self.reopen()
        elif st_results[6] < self._position:
            logger.info('file truncated, reopening...')
            self._read_from_beginning = True
            self.reopen()

    def read(self):
        try:
            self.check_file_sanity()
        except BaseException as be:
            self._read_from_beginning = True
            logger.error(be)
            return
        line = self._fd.readline()
        if not line:
            self._fd.seek(self._position)
            return None
        self._position = self._fd.tell()
        return line.strip()

