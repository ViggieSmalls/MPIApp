import os
from threading import Timer
import pandas as pd

class ProcessTable:

    def __init__(self, path, stop_event):
        self.t = 10 #seconds
        self.file = os.path.join(path, 'process_table.csv')
        self.micrographs = {}
        self.df = pd.DataFrame()
        self.thread = Timer(self.t, self.dump)
        self.thread.start()
        self.stop_event = stop_event

    def addMic(self, micrograph_name, results):
        self.micrographs[micrograph_name] = results

    def dump(self):
        for mic, res in self.micrographs.items():
            series = pd.Series(res, name=mic)
            self.df.append(series)
        self.df = self.df.sort_values(by='created_at')
        self.df.to_csv(self.file)
        self.thread = Timer(self.t, self.dump)

        if (not self.stop_event.is_set()):
            self.thread.start()
        else:
            self.thread.cancel()
