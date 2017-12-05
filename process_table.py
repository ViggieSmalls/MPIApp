import os
from threading import Timer
import pandas as pd

class ProcessTable:

    def __init__(self, path, stop_event):
        self.t = 20 # refresh time in seconds
        self.file = os.path.join(path, 'process_table.csv')
        # lists are thread safe
        self.series = []
        self.columns = set()
        self.stop_event = stop_event
        self.dump(self.t)

    def addMic(self, micrograph_name, results):
        # store processing results as a pandas.Series object
        self.series.append(pd.Series(results, name=micrograph_name))
        self.columns = self.columns | results.keys()

    def dump(self, t):
        df = pd.DataFrame(columns=self.columns)
        for series in self.series:
            df = df.append(series)
        if not df.empty:
            df = df.sort_values(by='created_at')
            print("Writing results to process table.")
            df.to_csv(self.file, index_label='micrograph')

        if self.stop_event.is_set():
            return
        self.thread = Timer(t, self.dump)
        self.thread.start()
