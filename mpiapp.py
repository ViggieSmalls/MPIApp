import argparse
import os
import gpustat
from flask import Flask, render_template
import pyinotify
from queue import Queue
from threading import Thread, Event
import shutil

from config_parser import ConfigParser
from process_table import ProcessTable
from micrograph import Micrograph

def main(config_file: str):
    conf = ConfigParser(config_file)
    queue = Queue()
    watch_manager = pyinotify.WatchManager()
    threads_stop = Event()

    process_table = ProcessTable(conf.output_directory, threads_stop)
    #FIXME: change config file parameter names to match variables
    handler = EventHandler(queue=queue, pattern=conf.file_extesion)
    notifier = pyinotify.ThreadedNotifier(watch_manager, handler)
    notifier.daemon = True
    notifier.start()

    watch_manager.add_watch(conf.input_directory, pyinotify.ALL_EVENTS)

    worker_kwargs = {
        'results_directory': conf.output_directory,
        'queue': queue,
        'stop_event': threads_stop,
        'motioncor_options': conf.motioncor_options,
        'gctf_options': conf.gctf_options,
        'process_table': process_table
    }

    gpu_threads = [Thread(target=worker, args=(i,), kwargs=worker_kwargs) for i in conf.GPUs]
    for thread in gpu_threads:
        thread.daemon = True
        thread.start()

    while True:
        user_input = input("Type 'quit' to stop processing:")
        if user_input == "quit":
            new_input = input("Do you want to quit? Any new incoming files will not be processed. (y/[n])")
            if new_input == "y":
                threads_stop.set()
                # TODO wait until everything stopped processing
                break


class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, **kwargs):
        self.queue = kwargs['queue']
        self.pattern = kwargs['pattern']
    def process_IN_CLOSE_WRITE(self, event):
        if os.path.splitext(event.pathname)[1] == self.pattern:
            # FIXME write to log
            print('Found new micrograph {}. Inserting in queue'.format(event.name))
            mic = Micrograph(event.pathname)
            self.queue.put(mic)

def worker(gpu_id, results_directory, queue, stop_event, motioncor_options, gctf_options, process_table):
    while (not stop_event.is_set()):
        # get a Micrograph object form the queue
        mic = queue.get()
        mic.motioncor_options = motioncor_options.copy()
        mic.gctf_options = gctf_options.copy()
        mic.process(gpu_id)
        mic.move_to_output_directory(results_directory)
        results = {**mic.motioncor_results, **mic.gctf_results, 'created_at': mic.created_at}
        process_table.addMic(mic.basename, results)


def run_server(port: int, static_folder: str):
    server = Flask(__name__)

    @server.route('/')
    def home():
        return render_template('start.html', projects=[[list('abc')]])

    server.run(host='0.0.0.0', port=port, static_folder=static_folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A task processing automation tool")
    parser.add_argument("config_file", help="Configuration file")
    args = parser.parse_args()

    main(config_file=args.config_file)