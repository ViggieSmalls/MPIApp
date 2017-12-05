import argparse
import os
from flask import Flask, render_template
import pyinotify
from queue import Queue
from threading import Thread, Event


from config_parser import ConfigParser
from process_table import ProcessTable
from micrograph import Micrograph

def main(conf, threads_stop):
    queue = Queue()
    watch_manager = pyinotify.WatchManager()

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
    #TODO watch_manager stop event


class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, **kwargs):
        self.queue = kwargs['queue']
        self.pattern = kwargs['pattern']
    def process_IN_CLOSE_WRITE(self, event):
        if os.path.splitext(event.pathname)[1] == self.pattern:
            # FIXME write to log
            print('New micrograph: {}. Inserting in queue.'.format(event.name))
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
    parser.add_argument("--test", help="Run test on the 20S Proteasome data set.", action="store_true")
    args = parser.parse_args()
    configurations = ConfigParser(args.config_file)
    stop_event = Event()

    if not args.test:
        main(conf=configurations, threads_stop=stop_event)

    elif args.test:
        import shutil
        from time import sleep
        import glob

        def copyfile_slow(files, dest):
            sleep(2) # wait for main to set up everything
            while bool(files):
                file = files.pop()
                shutil.copy(file, dest)
                sleep(60)

        myPath = os.path.dirname(os.path.abspath(__file__))
        loc_test_data = os.path.join(os.path.abspath('.'), "tests/data")
        assert os.path.isdir(loc_test_data), "Test data is expected to be loacted at {}".format(loc_test_data)
        configurations.input_directory = '/tmp/test_mpiapp_input'
        configurations.output_directory = '/tmp/test_mpiapp_output'
        os.mkdir(configurations.input_directory)
        os.mkdir(configurations.output_directory)
        data = sorted(glob.glob1(loc_test_data, '*.tif'))
        data = [os.path.join(loc_test_data, item) for item in data]
        main_thread = Thread(target=main, kwargs={'conf':configurations, 'threads_stop':stop_event})
        main_thread.start()
        copyfile_slow(data, configurations.input_directory)

    while True:
        if args.test:
            print("Test run finished.")
            user_input = input("Do you want to delete input and output directories? (y/[n]):")
            stop_event.set()
            if user_input == 'y':
                shutil.rmtree(configurations.input_directory)
                shutil.rmtree(configurations.output_directory)
                break
            elif user_input == 'n':
                break

        elif not args.test:
            user_input = input("Type 'quit' to stop processing:")
            if user_input == "quit":
                new_input = input("Do you want to quit? Any new incoming files will not be processed. (y/[n]):")
                if new_input == "y":
                    stop_event.set()
                    break