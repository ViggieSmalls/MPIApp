import os
from threading import Timer
import pandas as pd

class ProcessTable:

    def __init__(self, path, stop_event):
        self.t = 10 #seconds
        self.file = os.path.join(path, 'process_table.csv')
        self.micrographs = {}
        self.columns = set()
        self.stop_event = stop_event
        self.dump()

    def addMic(self, micrograph_name, results):
        self.micrographs[micrograph_name] = results
        self.columns = self.columns | results.keys()

    def dump(self):
        if self.stop_event.is_set():
            return
        df = pd.DataFrame(columns=self.columns)
        for mic, res in self.micrographs.items():
            series = pd.Series(res, name=mic)
            df = df.append(series)
        if not df.empty:
            df = df.sort_values(by='created_at')
            print("Writing results to process table.")
            df.to_csv(self.file)
        self.thread = Timer(self.t, self.dump)
        self.thread.start()


