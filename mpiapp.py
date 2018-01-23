import argparse
import logging
import os
import pyinotify
from queue import Queue
from threading import Thread, Event
from config_parser import ConfigParser
from process_table import ProcessTable
from micrograph import Micrograph
from time import sleep

def main(conf, files=None):
    stop_event = Event()
    queue = Queue()
    watch_manager = pyinotify.WatchManager()

    process_table = ProcessTable(conf.output_directory, stop_event)
    handler = EventHandler(queue=queue, pattern=conf.file_extesion)
    notifier = pyinotify.ThreadedNotifier(watch_manager, handler)
    notifier.daemon = True
    notifier.start()

    if os.path.isdir(conf.input_directory):
        logger.info('Adding watch to directory {}'.format(conf.input_directory))
        watch_manager.add_watch(conf.input_directory, pyinotify.ALL_EVENTS)
    else:
        logger.error('Input directory does not exist')

    worker_kwargs = {
        'results_directory': conf.output_directory,
        'queue': queue,
        'stop_event': stop_event,
        'motioncor_options': conf.motioncor_options,
        'gctf_options': conf.gctf_options,
        'process_table': process_table
    }
    logger.info('Worker thread arguments: {}'.format(worker_kwargs))

    logger.info('Starting {} worker threads'.format(len(conf.GPUs)))
    gpu_threads = [Thread(target=worker, args=(i,), kwargs=worker_kwargs) for i in conf.GPUs]
    for thread in gpu_threads:
        try:
            thread.daemon = True
            thread.start()
        except:
            logger.error('Worker threads could not be initialized')

    Thread(target=add_files_to_queue, args=(files, queue), daemon=True).start()

    user_input = input("Running MPIApp\nType 'quit' to stop processing:")
    if user_input == "quit":
        new_input = input("Are you sure you want to quit? (y/[n])")
        if new_input == "y":
            logger.info('Setting stop event')
            stop_event.set()
            notifier.stop()

def add_files_to_queue(files, queue):
    if bool(files):
        for file in files:
            queue.put(Micrograph(file))
            sleep(0.5)  # wait a little until all Micrograph class is initialized

class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, **kwargs):
        """
        called by parent class
        """
        self.logger = logging.getLogger('mpi_application')
        self.queue = kwargs['queue']
        self.pattern = kwargs['pattern']
        self.logger.info('EventHandler was initialized')
        self.logger.info('Watching for all events with the file extension {}'.format(self.pattern))

    def process_IN_CLOSE_WRITE(self, event):
        """
        all events that finished writing and have the specified extension are added to the queue
        """
        if os.path.splitext(event.pathname)[1] == self.pattern:
            self.logger.info('New micrograph: {}. Inserting in queue.'.format(event.name))
            mic = Micrograph(event.pathname)
            self.queue.put(mic)

def worker(gpu_id: int, results_directory, queue, stop_event, motioncor_options, gctf_options, process_table):
    """
    Gets event names from the queue and starts processing them
    :param gpu_id: all steps required to process a micrograph are done on one GPU
    :param results_directory: path to output directory
    :param queue: queue from which recent event names are stored for processing
    :param stop_event: kills the thread if set
    :param motioncor_options: motioncor parameters
    :param gctf_options: gctf parameters
    :param process_table: ProcessTable object. prevents simultaneous writing to a single csv file
    """
    while (not stop_event.is_set()):
        mic = queue.get()
        mic.motioncor_options = motioncor_options.copy()
        mic.gctf_options = gctf_options.copy()
        mic.process(gpu_id)
        # TODO: implement moving files and getting results inside the mic.process function
        mic.move_to_output_directory(results_directory)
        try:
            os.rmdir(mic.process_dir)
        except OSError:
            #FIXME log warning message
            pass
        results = {**mic.motioncor_results, **mic.gctf_results, 'created_at': mic.created_at}
        process_table.addMic(mic.basename, results)
    if stop_event.is_set():
        logger.info("Worker thread for GPU {} was shut down".format(str(gpu_id)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A task processing automation tool")
    parser.add_argument("config_file", help="Configuration file")
    parser.add_argument("--files", help="Provide data to process queue", nargs='+')
    args = parser.parse_args()

    configurations = ConfigParser(args.config_file)

    logger = logging.getLogger('mpi_application')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(configurations.logfile)
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info('Configuration files were read successfully from file {}'.format(os.path.abspath(args.config_file)))
    logger.info('Starting MPIApp')

    main(conf=configurations, files=args.files)
