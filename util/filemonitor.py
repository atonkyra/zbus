import os
import time
import logging

logger = logging.getLogger('FileMonitor')

class FileMonitor(object):
    def __init__(self, file):
        self._running = False
        self._filehandle = None
        self._file = file

    def read(self):
        st_results = None
        st_size = None
        ino = None

        while self._filehandle is None:
            try:
                self._filehandle = open(self._file, 'r')
                st_results = os.stat(self._file)
                st_size = st_results[6]
                ino = os.stat(self._filehandle.name).st_ino
                self._filehandle.seek(st_size)
            except:
                logger.error('failed to open %s, retrying in 1 second...', self._file)
                self._filehandle = None
                time.sleep(1)

        logger.info('listening for changes in file: %s', self._file)

        while True:
            gotline = False
            test_ino = ino
            try:
                test_ino = os.stat(self._filehandle.name).st_ino
            except:
                logger.error('failed to stat %s, retrying in 1 second...', self._file)
                time.sleep(1)
                continue

            if ino != test_ino:
                try:
                    if self._filehandle is not None:
                        self._filehandle.close()
                        self._filehandle = None
                    self._filehandle = open(self._file, 'r')
                except:
                    logger.error('failed to open %s, retrying in 1 second...', self._file)
                    time.sleep(1)
                    continue
                st_results = os.stat(self._file)
                st_size = st_results[6]
                ino = os.stat(self._filehandle.name).st_ino

            where = self._filehandle.tell()
            line = self._filehandle.readline()
            if not line:
                self._filehandle.seek(where)
            else:
                gotline = True

            if gotline:
                yield line.strip()
            else:
                time.sleep(0.1)
