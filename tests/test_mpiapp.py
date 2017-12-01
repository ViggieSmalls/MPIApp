import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

from mpiapp import EventHandler, worker
from queue import Queue
import pyinotify
import os
from time import sleep
from micrograph import Micrograph, File
from threading import Thread, Event

class TestEventHandler:
    def setup_method(self):
        self.queue = Queue()
        self.pattern = '.txt'
        handler = EventHandler(queue=self.queue, pattern=self.pattern)
        self.wm = pyinotify.WatchManager()
        self.notifier = pyinotify.ThreadedNotifier(self.wm, handler)
        self.notifier.daemon = True
        self.notifier.start()

    def teardown_method(self):
        self.notifier.stop()

    def test_new_events(self, tmpdir):
        path = str(tmpdir)
        file_name = 'testfile.txt'
        self.wm.add_watch(path, pyinotify.ALL_EVENTS)
        f = open(os.path.join(path, file_name), 'a').close()
        sleep(0.5) # wait for some time for the EventHandler to put it in the queue
        assert not self.queue.empty()

class TestWorkerFunction:
    def setup_method(self):
        self.queue = Queue()
        self.mic = Micrograph('WorkerFunction.tif')
        self.queue.put(self.mic)
        self.stop_event = Event()
        # FIXME: create a separate test: test_start_worker_function
        worker_kwargs = {
            'results_directory': None,
            'queue': self.queue,
            'stop_event': self.stop_event,
            'motioncor_options': None,
            'gctf_options': None,
            'process_table': None
        }
        self.thread = Thread(target=worker, args=(0, ), kwargs=worker_kwargs)
        self.thread.start()

    def teardown_method(self):
        self.thread.join()

    def test_worker_function(self):
        self.stop_event.set()

class TestFileObject:
    def test_move_to_new_location(self, tmpdir):
        old_path = str(tmpdir)
        file_name = 'FileObject.txt'
        f = open(os.path.join(old_path, file_name), 'a').close()
        file = File(os.path.join(old_path, file_name))
        new_folder_path = os.path.join(old_path, 'new_directory')
        os.mkdir(new_folder_path)
        file.move_to_directory(new_folder_path)
        assert file.abspath == os.path.join(new_folder_path, file_name)