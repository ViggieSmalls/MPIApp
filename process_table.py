import os
from threading import Timer

class ProcessTable:

    def __init__(self, path):
        self.t = 10 #seconds
        self.file = os.path.join(path, 'process_table.csv')
        self.micrographs = {}
        self.thread = Timer(self.t, self.dump)

    def addMic(self, micrograph_name, MicrographObject):
        self.micrographs[micrograph_name] = MicrographObject

    def dump(self):
        # create df from dict
        self.thread = Timer(self.t, self.dump)
        self.thread.start()

    def stop(self):
        self.thread.cancel()