import pyinotify
import os
from functions import motioncor, gctf, my_parser
import shutil
from threading import Thread
from queue import Queue
import re
import pandas as pd
from tkinter import *
from tkinter import filedialog, messagebox


class EventHandler(pyinotify.ProcessEvent):

    def my_init(self, **kwargs):
        self.process_queue = kwargs['process_queue']
        self.pattern = re.compile(kwargs['pattern'])

    def process_IN_CLOSE_WRITE(self, event):
        if re.match(self.pattern, event.name) :
            self.process_queue.put(event.name)


class MPIApp():
    def __init__(self, master):
        self.master = master
        self.master.title('MPIApp')
        self.create_widgets()
        # for test purposes
        #self.static_folder = '/home/viktor/PycharmProjects/MPIApp_v0.1/static'
        self.static_folder = '/var/www/MPIApp/MPIApp/static'
        self.processed_mics_dir_name = 'micrographs'
        self.GPUs = [0, 1]
        self.process_queue = Queue()
        self.results_list = []

    def dump_process_table(self, filepath: str):
        gctf_results = [item[1] for item in self.results_list]
        df = pd.DataFrame(gctf_results, columns=['Defocus_U', 'Defocus_V', 'Angle', 'Phase_shift', 'Resolution'])
        df.sort_index(inplace=True)
        df.to_csv(filepath)
        print('writing to process table')
        self.master.after(5000, self.dump_process_table, filepath)

    def start_worker_threads(self, GPUs: list, queue: Queue, config: dict, workdir: str, outputdir: str, static_folder: str, results: list):

        def worker(gpu_id: int):
            os.chdir(workdir)
            while True:
                micrograph = queue.get()
                mcorr_result = None
                gctf_result = None
                n_trials = 3
                print('starting to process', micrograph)
                while n_trials != 0:
                    result = motioncor(micrograph=micrograph,
                              config=config,
                              output_dir=outputdir,
                              static_dir=static_folder,
                              gpu_id=gpu_id)
                    if not result.empty:
                        mcorr_result = result
                        break
                    else:
                        n_trials -= 1
                n_trials = 3
                while n_trials != 0:
                    result = gctf(micrograph=micrograph,
                         config=config,
                         output_dir=outputdir,
                         static_dir=static_folder,
                         gpu_id=gpu_id)
                    if not result.empty:
                        gctf_result = result
                        break
                    else:
                        n_trials -= 1
                results.append([mcorr_result, gctf_result])
                shutil.move(micrograph, os.path.join(outputdir, self.processed_mics_dir_name))
                print('finished', micrograph)

        gpu_threads = [Thread(target=worker, args=(i, )) for i in GPUs]
        for thread in gpu_threads:
            thread.daemon = True
            thread.start()

    def start_event_notifier(self, directory: str, queue: Queue, pattern):
        wm = pyinotify.WatchManager()
        notifier = pyinotify.ThreadedNotifier(wm, EventHandler(process_queue=queue, pattern=pattern))
        notifier.daemon = True
        notifier.start()
        wm.add_watch(directory, pyinotify.ALL_EVENTS)

    def create_widgets(self):
        # INPUT_FILES_DIRECTORY
        self.labelWorkdir = Label(self.master, text="Input files directory")
        self.labelWorkdir.grid(row=0, column=0)
        self.entryWorkdir = Entry(self.master, width=30, text="")
        self.entryWorkdir.grid(row=0, column=1)
        self.btnWorkdir = Button(self.master, text="...", command=self.button_INPUT_FILES_DIRECTORY)
        self.btnWorkdir.grid(row=0, column=2)

        # PROCESSED_OUTPUT_DIRECTORY
        self.labelOutputDir = Label(self.master, text="Processed output directory")
        self.labelOutputDir.grid(row=1, column=0)
        self.entryOutputDir = Entry(self.master, width=30, text="")
        self.entryOutputDir.grid(row=1, column=1)
        self.btnOutputDir = Button(self.master, text="...", command=self.button_PROCESSED_OUTPUT_DIRECTORY)
        self.btnOutputDir.grid(row=1, column=2)

        # PROJECT_NAME
        self.labelProjectName = Label(self.master, text="Project name")
        self.labelProjectName.grid(row=2, column=0)
        self.entryProjectName = Entry(self.master, width=30, text="")
        self.entryProjectName.grid(row=2, column=1)

        # CONFIGURATION_FILE
        self.labelConfig = Label(self.master, text="Configuration File")
        self.labelConfig.grid(row=3, column=0)
        self.entryConfig = Entry(self.master, width=30, text="")
        self.entryConfig.grid(row=3, column=1)
        self.btnConfig = Button(self.master, text="...", command=self.button_CONFIGURATION_FILE)
        self.btnConfig.grid(row=3, column=2)

        # PATTERN
        self.PATTERN = StringVar(self.master)
        self.PATTERN.set('.*\.tif')
        self.labelPattern = Label(self.master, text="Input files extension")
        self.labelPattern.grid(row=4, column=0)
        self.optboxPattern = OptionMenu(self.master, self.PATTERN, '.*\.tif', '.*\.mrc')
        self.optboxPattern.grid(row=4, column=1)

        # TODO: GPUs

        # Start processing
        self.btnStartWatch = Button(self.master, text="Start", command=self.button_start_processing)
        self.btnStartWatch.grid(row=5, column=0)

        self.entryWorkdir.insert(0, '/home/viktor/Downloads/20s_test_data/new')
        self.entryOutputDir.insert(0, '/home/viktor/Downloads/20s_test_data/output')
        self.entryProjectName.insert(0, 'NewProject')
        self.entryConfig.insert(0, '/home/viktor/PycharmProjects/MPIApp/simple.cfg')

    def button_INPUT_FILES_DIRECTORY(self):
        dirname = filedialog.askdirectory(title="Select directory to watch")
        if dirname:
            self.entryWorkdir.delete(0, END)  # delete previous entry
            self.entryWorkdir.insert(0, dirname)

    def button_PROCESSED_OUTPUT_DIRECTORY(self):
        dirname = filedialog.askdirectory(title="Select output directory")
        if dirname:
            self.entryOutputDir.delete(0, END)  # delete previous entry
            self.entryOutputDir.insert(0, dirname)

    def button_CONFIGURATION_FILE(self):
        config_file = filedialog.askopenfilenames(title="Select configuration file")
        if config_file:
            self.entryConfig.delete(0, END)  # delete previous entry
            self.entryConfig.insert(0, config_file)

    def button_start_processing(self):
        INPUT_FILES_DIRECTORY = self.entryWorkdir.get()
        PROCESSED_OUTPUT_DIRECTORY = self.entryOutputDir.get()
        CONFIGURATION_FILE = self.entryConfig.get()
        PROJECT_NAME = self.entryProjectName.get()
        project_static_folder = os.path.join(self.static_folder, 'data', PROJECT_NAME)

        assert os.path.isdir(INPUT_FILES_DIRECTORY), messagebox.showerror("Error","Input directory does not exist")
        assert os.path.isdir(PROCESSED_OUTPUT_DIRECTORY), messagebox.showerror("Error","Output directory does not exist")
        assert os.path.isfile(CONFIGURATION_FILE), messagebox.showerror("Error","Configuration file does not exist")
        assert PROJECT_NAME != None, messagebox.showerror("Error","You must specify a project name")
        assert not os.path.isdir(project_static_folder), messagebox.showerror("Error","The project name '{}' already exists".format(PROJECT_NAME))

        process_table_file = os.path.join(project_static_folder, 'process_table.csv')
        processed_mics_dir = os.path.join(PROCESSED_OUTPUT_DIRECTORY, self.processed_mics_dir_name)

        try:
            os.mkdir(project_static_folder)
            os.mkdir(processed_mics_dir)
        except FileExistsError:
            pass
        config = my_parser(CONFIGURATION_FILE)
        shutil.copy(CONFIGURATION_FILE, project_static_folder)

        self.dump_process_table(process_table_file)
        self.start_event_notifier(directory=INPUT_FILES_DIRECTORY, queue=self.process_queue, pattern=self.PATTERN.get())
        self.start_worker_threads(GPUs=self.GPUs,
                                  queue=self.process_queue,
                                  config=config,
                                  workdir=INPUT_FILES_DIRECTORY,
                                  outputdir=PROCESSED_OUTPUT_DIRECTORY,
                                  static_folder=project_static_folder,
                                  results=self.results_list)

        # TODO: create a button 'stop processing'
        # self.btnStartWatch.configure(text="Stop", bg="red", command=self.stop_processing)


root = Tk()
app = MPIApp(root)
root.mainloop()
