import argparse
import logging
import os
import pyinotify
from queue import Queue
from threading import Thread, Event
from config_parser import ConfigParser
from process_table import ProcessTable
from micrograph import Micrograph

def main(conf, files):
    stop_event = Event()
    queue = Queue()
    watch_manager = pyinotify.WatchManager()

    for file in files:
        queue.put(file)

    process_table = ProcessTable(conf.output_directory, stop_event)

    logger.info('Configuring EventHandler')
    handler = EventHandler(queue=queue, pattern=conf.file_extesion)
    notifier = pyinotify.ThreadedNotifier(watch_manager, handler)
    notifier.daemon = True
    notifier.start()
    logger.info('Adding watch to directory {}'.format(conf.input_directory))
    watch_manager.add_watch(conf.input_directory, pyinotify.ALL_EVENTS)

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

    user_input = input("Type 'quit' to stop processing.")
    if user_input == "quit":
        new_input = input("Are you sure you want to quit? (y/[n])")
        if new_input == "y":
            logger.info('Setting stop event')
            stop_event.set()
            logger.info('')
            notifier.stop()
            logger.info('All MPIApp threads were successfully shut down')


class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, **kwargs):
        """
        called by parent class
        """
        self.logger = logging.getLogger('mpi_application')
        self.queue = kwargs['queue']
        self.pattern = kwargs['pattern']
        self.logger.info('Watching for all events with the file extension {}'.format(self.pattern))

    def process_IN_CLOSE_WRITE(self, event):
        """
        all events that finished writing and have the specified extension are added to the queue
        """
        if os.path.splitext(event.pathname)[1] == self.pattern:
            # FIXME write to log
            print('New micrograph: {}. Inserting in queue.'.format(event.name))
            mic = Micrograph(event.pathname)
            self.queue.put(mic)

def worker(gpu_id, results_directory, queue, stop_event, motioncor_options, gctf_options, process_table):
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
        mic.move_to_output_directory(results_directory)
        results = {**mic.motioncor_results, **mic.gctf_results, 'created_at': mic.created_at}
        process_table.addMic(mic.basename, results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A task processing automation tool")
    parser.add_argument("config_file", help="Configuration file")
    parser.add_argument("--files", help="Provide data to process queue", nargs='+')
    args = parser.parse_args()

    logger = logging.getLogger('mpi_application')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('mpiapp.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)

    stop_event = Event()
    logger.info('Reading configurations from file {}'.format(os.path.abspath(args.config_file)))
    configurations = ConfigParser(args.config_file)
    logger.info('Configuration files were read successfully')

    logger.info('Starting MPIApp in process mode')
    main(conf=configurations, files=args.files)
