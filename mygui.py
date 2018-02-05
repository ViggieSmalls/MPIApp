import os
import sys
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QMessageBox
from gui import Ui_MainWindow
import logging
import pyinotify
import subprocess
import shutil
import json
import numpy as np
import pandas as pd
from queue import Queue
from threading import Thread, Lock, Event
from functools import partial
import matplotlib.pyplot as plt
import datetime
import mrcfile
from scipy import misc
from skimage import exposure

class MPIApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.logger = logging.getLogger(__name__)

        # button actions
        self.ui.btnInputDir.clicked.connect(self.select_input_directory)
        self.ui.btnOutputDir.clicked.connect(self.select_output_directory)
        self.ui.btnGain.clicked.connect(self.select_gain)
        self.ui.btn_Run.clicked.connect(self.accept)
        self.ui.btn_Exit.clicked.connect(self.exit)
        self.ui.btn_motioncor_executable.clicked.connect(self.select_motioncor_executable)
        self.ui.btn_gctf_executable.clicked.connect(self.select_gctf_executable)
        self.ui.actionLoad_configurations.triggered.connect(partial(self.select_configuration_file, directory='.'))
        self.ui.actionSave_configurations.triggered.connect(partial(self.save_configurations, autosave=False))
        self.ui.actionExit.triggered.connect(self.exit)

        # line values connected to each other
        self.ui.line_kV.textChanged.connect(self.sync_motioncor_kV)
        self.ui.line_kV.textChanged.connect(self.sync_gctf_kV)
        self.ui.line_apix.textChanged.connect(self.sync_motioncor_PixSize)
        self.ui.line_apix.textChanged.connect(self.sync_gctf_apix)
        self.ui.line_dose_per_frame.textChanged.connect(self.sync_motioncor_FmDose)
        self.ui.line_cs.textChanged.connect(self.sync_gctf_cs)
        self.ui.line_ac.textChanged.connect(self.sync_gctf_ac)
        self.ui.motioncor_FtBin.textChanged.connect(self.sync_FtBin_changes_gctf_apix)

        # load default values
        config = json.load(open('base_config.json'))
        self.main_defaults = {
            'InputDir': os.path.abspath('.'),
            'OutputDir': os.path.join(os.path.abspath('.'), 'output'),
            'Gain': ''
        }
        self.main_defaults.update(config["Main"])
        self.motioncor_defaults = config["Motioncor"]
        self.gctf_defaults = config["Gctf"]

        # set placeholder text of the lineEdit objects
        self.set_placeholder_text()

        # list latest configurations
        self.ui.menuLatest_configurations = QtWidgets.QMenu(self.ui.menuFile)
        self.ui.actionRecent_configurations.setMenu(self.ui.menuLatest_configurations)

        self.config_dir = os.path.join(os.path.dirname(__file__), 'last_configs')
        if not os.path.isdir(self.config_dir):
            os.mkdir(self.config_dir)

        files = os.listdir(self.config_dir)
        files.sort(reverse=True) # latest first
        for n, filename in enumerate(files):
            obj_name = 'file_' + str(n)
            setattr(self.ui.menuLatest_configurations, obj_name, QtWidgets.QAction(self))
            action = getattr(self.ui.menuLatest_configurations, obj_name)
            action.setText(filename)
            self.ui.menuLatest_configurations.addAction(action)
            action.triggered.connect(partial(self.load_configurations, filename=os.path.join(self.config_dir, filename)))

            if n>=4:
                self.ui.menuLatest_configurations.addSeparator()
                text = 'Show all...'
                setattr(self.ui.menuLatest_configurations, 'actionShow_all', QtWidgets.QAction(self))
                action_show_all = getattr(self.ui.menuLatest_configurations, 'actionShow_all')
                action_show_all.setObjectName('actionShow_all')
                action_show_all.setText(text)
                self.ui.menuLatest_configurations.addAction(action_show_all)
                action_show_all.triggered.connect(partial(self.select_configuration_file, directory=self.config_dir))
                break

        self.motioncor_options = {}
        self.gctf_options = {}

    def sync_motioncor_kV(self, text):
        self.ui.motioncor_kV.setText(text)

    def sync_gctf_kV(self, text):
        self.ui.gctf_kV.setText(text)

    def sync_motioncor_PixSize(self, text):
        self.ui.motioncor_PixSize.setText(text)

    def sync_gctf_apix(self, text):
        """
        This function changes the text in the gctf_apix line whenever line_apix is edited
        """
        try:
            str_ftbin = self.ui.motioncor_FtBin.text()
            ftbin = self.motioncor_defaults['FtBin'] if str_ftbin=='' else float(str_ftbin)
            apix = self.gctf_defaults['apix'] if text=='' else float(text)
            new_apix = ftbin * apix
            self.ui.gctf_apix.setText(str(new_apix))
        except:
            self.ui.gctf_apix.setText(text) #catch exception later

    def sync_FtBin_changes_gctf_apix(self, text):
        try:
            ftbin = self.motioncor_defaults['FtBin'] if text=='' else float(text)
            str_apix = self.ui.line_apix.text()
            apix = self.gctf_defaults['apix'] if str_apix=='' else float(str_apix)
            new_gctf_apix = ftbin * float(apix)
            self.ui.gctf_apix.setText(str(new_gctf_apix))
        except:
            pass

    def sync_motioncor_FmDose(self, text):
        self.ui.motioncor_FmDose.setText(text)

    def sync_gctf_cs(self, text):
        self.ui.gctf_cs.setText(text)

    def sync_gctf_ac(self, text):
        self.ui.gctf_ac.setText(text)

    def select_input_directory(self):
        directory = str(QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory"))
        self.ui.line_InputDir.setText(directory)

    def select_output_directory(self):
        directory = str(QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory"))
        self.ui.line_OutputDir.setText(directory)

    def select_gain(self):
        file = str(QtWidgets.QFileDialog.getOpenFileName(self, "Select File")[0])
        self.ui.line_Gain.setText(file)

    def select_configuration_file(self, directory):
        filename = QtWidgets.QFileDialog.getOpenFileName(self, 'Open File', directory, "Config (*.json)")[0]
        if os.path.isfile(filename):
            self.load_configurations(filename)

    def load_configurations(self, filename):
        config = json.load(open(filename))
        if "Main" in config:
            for param, value in config["Main"].items():
                if hasattr(self.ui, 'line_' + param):
                    line = getattr(self.ui, 'line_' + param)
                    if type(value) == str:
                        line.setText(value)
                    elif type(value) == int or type(value) == float:
                        line.setText(str(value))
                    elif type(value) == list:
                        line.setText(' '.join(map(str, value)))

        if "Motioncor" in config:
            m_string = ""
            for param, value in config["Motioncor"].items():
                # convert value to string
                if type(value) == int or type(value) == float:
                    value_as_string = str(value)
                elif type(value) == list:
                    value_as_string =' '.join(map(str, value))
                else:
                    value_as_string = value

                # fill in the lineEdits or append to additional parameters
                if hasattr(self.ui, 'motioncor_' + param):
                    line = getattr(self.ui, 'motioncor_' + param)
                    line.setText(value_as_string)
                else:
                    m_string += "-{param} {value} ".format(param=param, value=value_as_string)
            self.ui.plainTextEdit_motioncor.setPlainText(m_string)

        if "Gctf" in config:
            g_string = ""
            for param, value in config["Gctf"].items():
                # convert value to string
                if type(value) == int or type(value) == float:
                    value_as_string = str(value)
                elif type(value) == list:
                    value_as_string =' '.join(map(str, value))
                else:
                    value_as_string = value

                # fill in the lineEdits or append to additional parameters
                if hasattr(self.ui, 'gctf_' + param):
                    line = getattr(self.ui, 'gctf_' + param)
                    line.setText(value_as_string)
                else:
                    g_string += "--{param} {value} ".format(param=param, value=value_as_string)
            self.ui.plainTextEdit_gctf.setPlainText(g_string)

    # FIXME this can only save configurations if we press run.
    # get options as soon as typed in?
    def save_configurations(self, autosave):
        self.get_motioncor_options()
        self.get_gctf_options()
        config = {"Main":{}, "Motioncor": {}, "Gctf": {}}
        config["Main"] = self.main_defaults
        config["Motioncor"] = self.motioncor_options
        config["Gctf"] = self.gctf_options

        if autosave==True:
            date_string = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            config_filename = os.path.join(self.config_dir, date_string + '.json')
            with open(config_filename, 'w') as config_file:
                json.dump(config, config_file, indent=4, sort_keys=True, separators=(',', ': '))

        elif autosave==False:
            filename = QtWidgets.QFileDialog.getSaveFileName(self, 'Save File', os.path.join('.', 'my_config.json'), "Config (*.json)")[0]
            if filename != '':
                with open(filename, 'w') as config_file:
                    json.dump(config, config_file, indent=4, sort_keys=True, separators=(',', ': '))

    def select_motioncor_executable(self):
        self.motioncor_executable = str(QtWidgets.QFileDialog.getOpenFileName(self, "Select Executable")[0])

    def select_gctf_executable(self):
        self.gctf_executable = str(QtWidgets.QFileDialog.getOpenFileName(self, "Select Executable")[0])

    def get_GPUs(self):
        self.GPUs = []
        if self.ui.GPU_0.isChecked():
            self.GPUs.append(0)
        if self.ui.GPU_1.isChecked():
            self.GPUs.append(1)
        if self.ui.GPU_2.isChecked():
            self.GPUs.append(2)
        if self.ui.GPU_3.isChecked():
            self.GPUs.append(3)
        if self.ui.GPU_4.isChecked():
            self.GPUs.append(4)
        if self.ui.GPU_5.isChecked():
            self.GPUs.append(5)
        if self.ui.GPU_6.isChecked():
            self.GPUs.append(6)
        if self.ui.GPU_7.isChecked():
            self.GPUs.append(7)
        if self.ui.GPU_8.isChecked():
            self.GPUs.append(8)
        if self.ui.GPU_9.isChecked():
            self.GPUs.append(9)
        if len(self.GPUs) == 0:
            raise ValueError('You must select at least one GPU')

    def get_file_extension(self):
        if self.ui.radio_mrc.isChecked():
            self.file_extension = '.mrc'
            self.motioncor_options['InMrc'] = '{motioncor_input}'
        elif self.ui.radio_tif.isChecked():
            self.file_extension = '.tif'
            self.motioncor_options['InTiff'] = '{motioncor_input}'
        if not hasattr(self, 'file_extension'):
            raise ValueError('No file extension selected')

    def get_input_dir(self):
        self.inputDir = self.ui.line_InputDir.text()
        if self.inputDir == '':
            self.inputDir = self.main_defaults['InputDir']
        if not os.path.isdir(self.inputDir):
            raise ValueError('The input directory does not exist')

    def get_output_dir(self):
        """
        creates at maximum one subfoder to an existing directory
        """
        self.outputDir = self.ui.line_OutputDir.text()
        if self.outputDir == '':
            self.outputDir = self.main_defaults['OutputDir']
        if not os.path.isdir(self.outputDir):
            try:
                os.mkdir(self.outputDir)
            except Exception as ex:
                raise type(ex)(str(ex) + '(Output Directory)')

    def set_placeholder_text(self):
        # set placeholder text in the Main tab
        for param in self.main_defaults.keys():
            try:
                line = getattr(self.ui, 'line_{}'.format(param))
                value = self.main_defaults[param]
                if type(value) == list:
                    placeholder = ' '.join(map(str, value))
                else:
                    placeholder = str(value)
                line.setPlaceholderText(placeholder)
            except:
                pass # there is no such line attribute

        # set placeholder text in the Motioncor tab
        for param in self.motioncor_defaults.keys():
            try:
                line = getattr(self.ui, 'motioncor_{}'.format(param))
                value = self.motioncor_defaults[param]
                if type(value) == list:
                    placeholder = ' '.join(map(str, value))
                else:
                    placeholder = str(value)
                line.setPlaceholderText(placeholder)
            except:
                pass # there is no such line attribute

        # set placeholder text in the Gctf tab
        for param in self.gctf_defaults.keys():
            try:
                line = getattr(self.ui, 'gctf_{}'.format(param))
                value = self.gctf_defaults[param]
                if type(value) == list:
                    placeholder = ' '.join(map(str, value))
                else:
                    placeholder = str(value)
                line.setPlaceholderText(placeholder)
            except:
                pass # there is no such line attribute

    def get_motioncor_options(self):

        try:
            # get all line entries
            motioncor_lines = self.select_ui_elements_that_start_with('motioncor_')
            self.motioncor_options = {}
            for line in motioncor_lines:
                param = line.objectName()[len('motioncor_'):]
                value = line.text()
                if value != '':
                    self.motioncor_options[param] = value

            # get additional parameters in the advanced options
            additional_parameters = self.ui.plainTextEdit_motioncor.toPlainText()
            additional_parameters = additional_parameters.replace('\n', ' ')
            if additional_parameters != '':
                groups = additional_parameters.split('-')
                # filter out empty strings from list
                groups = list(filter(None, groups))
                for group in groups:
                    group = group.strip()
                    idx_first_space = group.index(' ')
                    try:
                        param = group[:idx_first_space]
                        value = group[idx_first_space:]
                        self.motioncor_options[param] = value
                    except Exception as ex:
                        raise type(ex)("Check motioncor parameter -{}".format(group))

            # convert to default type
            for key, value in self.motioncor_options.items():
                if key in self.motioncor_defaults:
                    try:
                        default_type = type(self.motioncor_defaults[key])
                        if default_type == list:
                            default_item_type = type(self.motioncor_defaults[key][0])
                            self.motioncor_options[key] = list(map(default_item_type, value.split()))
                            assert len(self.motioncor_options[key]) == len(self.motioncor_defaults[key])
                        else:
                            self.motioncor_options[key] = default_type(self.motioncor_options[key])

                    except Exception as ex:
                        raise type(ex)("Type of {} parameter is incorrect".format(key))
                else:
                    self.logger.warning('Unknown motioncor parameter: {}'.format(key))

            # check for essential motioncor parameters
            essential_keys = ['timeout', 'trials', 'kV', 'PixSize', 'FmDose'] #optionally also: 'Gain'
            for key in essential_keys:
                if key not in self.motioncor_options:
                    self.motioncor_options[key] = self.motioncor_defaults[key]

            self.get_file_extension()

        except Exception as ex:
            raise ex

    def get_gctf_options(self):

        try:
            gctf_lines = self.select_ui_elements_that_start_with('gctf_')
            self.gctf_options = {}
            for line in gctf_lines:
                param = line.objectName()[len('gctf_'):]
                value = line.text()
                if value != '':
                    self.gctf_options[param] = value

            # get additional parameters in the advanced options
            additional_parameters = self.ui.plainTextEdit_gctf.toPlainText()
            additional_parameters = additional_parameters.replace('\n', ' ')
            if additional_parameters != '':
                groups = additional_parameters.split('--')
                # filter out empty strings from list
                groups = list(filter(None, groups))
                for group in groups:
                    group = group.strip()
                    idx_first_space = group.index(' ')
                    try:
                        param = group[:idx_first_space]
                        value = group[idx_first_space:]
                        self.gctf_options[param] = value
                    except Exception as ex:
                        raise type(ex)("Check gctf parameter --{}".format(group))

            # convert to default type
            for key, value in self.gctf_options.items():
                if key in self.gctf_defaults:
                    try:
                        default_type = type(self.gctf_defaults[key])
                        if default_type == list:
                            default_item_type = type(self.gctf_defaults[key][0])
                            self.gctf_options[key] = list(map(default_item_type, value.split()))
                            assert len(self.gctf_options[key]) == len(self.gctf_defaults[key])
                        else:
                            self.gctf_options[key] = default_type(self.gctf_options[key])

                    except Exception as ex:
                        raise type(ex)("Type of {} parameter is incorrect".format(key))
                else:
                    self.logger.warning('Unknown gctf parameter: {}'.format(key))

            # check for essential gctf parameters
            essential_keys = ['timeout', 'trials', 'cc_cutoff', 'apix', 'kV', 'ac', 'cs']
            for key in essential_keys:
                if key not in self.gctf_options:
                    self.gctf_options[key] = self.gctf_defaults[key]

        except Exception as ex:
            raise ex

    def set_up_motioncor(self):
        self.get_motioncor_options()

        if 'Gain' not in self.motioncor_options:
            gain_reference = self.ui.line_Gain.text()
            if gain_reference == '':
                button_reply = QMessageBox.question(self, 'Warning', "You have not selected a gain reference", QMessageBox.Ignore | QMessageBox.Abort, QMessageBox.Ignore)
                if button_reply == QMessageBox.Ignore:
                    pass
                elif button_reply == QMessageBox.Abort:
                    raise Exception('Motioncor was not set up.')
            else:
                assert os.path.isfile(gain_reference), "Select a Gain reference"
                self.motioncor_options['Gain'] = gain_reference

        # set motioncor exectutable to motioncor, if not selected
        if not hasattr(self, 'motioncor_executable'):
            self.motioncor_executable = 'motioncor'
        assert shutil.which(self.motioncor_executable), "Select a motioncor executable"

        self.motioncor = Motioncor(self.logger, self.motioncor_options,
                                   self.outputDir, self.motioncor_executable)

    def set_up_gctf(self):
        self.get_gctf_options()

        # check gctf executable
        if not hasattr(self, 'gctf_executable'):
            self.gctf_executable = 'gctf'
        assert shutil.which(self.gctf_executable), "Select a gctf executable"

        self.gctf = Gctf(self.logger, self.gctf_options, self.outputDir, self.gctf_executable)

    def start_logging(self):
        """
        Start logging to 'mpiapp.log' inside the output directory
        If there is already a file handler, do nothing
        """
        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG)
            fh = logging.FileHandler(os.path.join(self.outputDir, 'mpiapp.log'))
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def start_process_queue(self):
        self.process_table = pd.DataFrame()
        self.process_table_lock = Lock()
        self.queue = Queue()

    def start_event_notifier(self):
        """
        Watch the input directory for new files that have the
        specified file extension
        """
        self.wm = pyinotify.WatchManager()
        self.notifier = pyinotify.ThreadedNotifier(self.wm, EventHandler(logger=self.logger, queue=self.queue, file_extension=self.file_extension))
        self.notifier.daemon = True
        self.notifier.start()
        self.wdd = self.wm.add_watch(self.inputDir, pyinotify.ALL_EVENTS)

    def worker(self, gpu_id):
        while (not self.stop_event.is_set()):
            micrograph = self.queue.get()
            if micrograph is None:
                # in case we want to stop the worker if queue is empty,
                # we just put None inside the Queue
                break
            else:
                self.logger.info('Processing Micrograph {}'.format(micrograph.basename))
                try:
                    self.motioncor(micrograph, gpu_id)
                    self.gctf(micrograph, gpu_id)
                    self.process_table_update(micrograph)

                    # move the micrograph after processing to $OUTPUT_DIR/frames
                    frames_dir = os.path.join(self.outputDir, 'frames')
                    if not os.path.isdir(frames_dir):
                        os.mkdir(frames_dir)
                    shutil.move(micrograph.files['raw'], frames_dir)
                except Exception as ex:
                    self.logger.error(str(ex))

        if self.stop_event.is_set():
            self.logger.info("Worker thread for GPU {} was shut down".format(str(gpu_id)))

    def start_worker_threads(self):
        """
        Start a thread for each GPU ID. Each thread will process one
        micrograph sequentially
        :return:
        """
        self.gpu_threads = [Thread(target=self.worker, args=(i,)) for i in self.GPUs]
        for thread in self.gpu_threads:
            self.logger.info('Starting thread for GPU with ID: {}'.format(thread._args[0]))
            thread.daemon = True
            thread.start()

    def process_table_update(self, micrograph):
        """
        Update the DatFrame with the micrograph data (Series object).
        """
        self.process_table_lock.acquire()
        self.process_table = self.process_table.append(micrograph.data)
        self.process_table_lock.release()

    def process_table_dump(self):
        """
        Write the data in the process table to a csv file
        and to a star file to use as input for relion
        """
        csv_file = os.path.join(self.outputDir, "process_table.csv")
        gctf_star = os.path.join(self.outputDir, 'micrographs_all_gctf.star')

        self.process_table_lock.acquire()

        # create some additional columns for the web page
        if not self.process_table.empty:
            # create histograms
            self.process_table.hist('Resolution', edgecolor='black', color='green')
            plt.xlabel('Resolution (\u212B)')
            plt.savefig(os.path.join(self.outputDir, 'histogram_resolution.png'))
            plt.close()
            self.process_table.hist('Defocus', edgecolor='black', color='blue')
            plt.xlabel('Defocus (\u03BCm)')
            plt.savefig(os.path.join(self.outputDir, 'histogram_defocus.png'))
            plt.close()

            ### write csv file
            self.logger.debug('Writing data to process table csv file')
            self.process_table.set_index('micrograph').sort_index().to_csv(csv_file)

            ### write star file

            # get star file header values
            # TODO: try to make this easier, like sort columns and then write to file
            self.logger.debug('Writing data to star file')
            _rln = self.process_table.filter(regex=("^_rln.*"))
            keys = list(_rln.columns)
            d = [i.split() for i in keys]
            d = [(i[0], int(i[1][1:])) for i in d]
            sorted_list = sorted(d, key=lambda x: x[1])
            columns = [i[0] + ' #' + str(i[1]) for i in sorted_list]

            # write star file content
            rln = _rln[columns]
            rln.to_csv(gctf_star, index=False, header=False, sep='\t')

            # write the star file header
            with open(gctf_star, 'r+') as f:
                content = f.read()
                f.seek(0, 0)
                f.write('data_\nloop_\n' + '\n'.join(columns) + '\n' + content)

        self.process_table_lock.release()

    def accept(self):
        try:
            self.get_GPUs()
            self.get_file_extension()
            self.get_input_dir()
            self.get_output_dir()
            self.set_up_motioncor()
            self.set_up_gctf()
            self.ui.btn_Run.setText('Abort')
            self.ui.btn_Run.clicked.disconnect()
            self.ui.btn_Run.clicked.connect(self.abort)
            disable_this = self.select_ui_elements_that_start_with('groupBox')
            for obj in disable_this:
                obj.setEnabled(False)
            self.run()
        except Exception as ex:
            QtWidgets.QMessageBox.about(self, 'ERROR', str(ex))

    def run(self):
        # reset the stop event in case we did an abort before
        self.stop_event = Event()
        self.save_configurations(autosave=True)

        # start everything
        self.start_logging()
        self.start_process_queue()
        self.start_event_notifier()
        self.start_worker_threads()

        # write data to csv file every 10 seconds
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.process_table_dump)
        self.timer.start(10000)

        # set status label
        self.ui.label_status.setText('Processing...')
        pass

    def abort(self):
        # set status label
        self.ui.label_status.setText('Killing worker threads')
        self.ui.label_status.repaint()

        # set stop events
        self.notifier.stop()
        self.stop_event.set()
        self.queue.queue.clear()

        # wait for all threads to finish before continuing
        for thread in self.gpu_threads:
            self.queue.put(None)
            thread.join()

        # stop writing to the process table
        self.timer.stop()

        # reset all the gui elements to normal
        self.ui.btn_Run.setText('Run')
        self.ui.btn_Run.clicked.disconnect()
        self.ui.btn_Run.clicked.connect(self.accept)
        enable_this = self.select_ui_elements_that_start_with('groupBox')
        for obj in enable_this:
            obj.setEnabled(True)

        # clear status label
        self.ui.label_status.setText('')
        pass

    def select_ui_elements_that_start_with(self, string):
        """
        selects all UI elements that start with the specified string
        :param string:
        :return: list of ui objects
        """
        objects = []
        for object_name in self.ui.__dict__.keys():
            if object_name.startswith(string):
                objects.append(getattr(self.ui, object_name))
        return objects

    def exit(self):
        sys.exit()

class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, **kwargs):
        """
        called by parent class
        """
        self.logger = kwargs['logger']
        self.queue = kwargs['queue']
        self.file_extension = kwargs['file_extension']
        self.logger.info('EventHandler was initialized')
        self.logger.info('Watching for all events with the file extension {}'.format(self.file_extension))

    def process_IN_CLOSE_WRITE(self, event):
        """
        all events that finished writing and have the specified extension are added to the queue
        """
        if os.path.splitext(event.pathname)[1] == self.file_extension:
            self.logger.info('New micrograph: {}. Inserting in queue.'.format(event.name))
            mic = Micrograph(event.pathname, self.logger)
            self.queue.put(mic)

class Gctf:
    def __init__(self, logger, options, output_directory, executable):
        self.logger = logger
        self.options = options
        self.executable = executable

        # create required folders
        self.output_dir = output_directory
        self.results_dir = os.path.join(self.output_dir, 'gctf')
        if not os.path.isdir(self.results_dir):
            os.makedirs(self.results_dir)
        self.static_dir = os.path.join(self.output_dir, 'static', 'gctf') # directory to which png files will be saved to
        if not os.path.isdir(self.static_dir):
            os.makedirs(self.static_dir)


    def __call__(self, micrograph, gpu_id: int):
        """
        Gctf is called as: executable [options] [file(s)]
        The micrograph is processed inside the directory where the input file resides
        Gctf can not be called on symbolic links

        :param micrograph: Micrograph object
        :param gpu_id: gpu ID on which the processing will occur
        :return:
        """

        # create a symbolic link to the input file inside the gctf directory
        assert 'gctf_input' in micrograph.files, "No gctf input file found for micrograph {}".format(micrograph.basename)
        gctf_input = os.path.join(self.results_dir, os.path.basename(micrograph.files['gctf_input']))
        os.symlink(micrograph.files['gctf_input'], gctf_input)

        # set up additional options
        options = self.options.copy()
        options['gid'] = gpu_id
        ctfstar = os.path.join(self.results_dir, micrograph.basename + '.star')
        options['ctfstar'] = ctfstar
        timeout = options.pop('timeout')
        trials = options.pop('trials')
        cc_cutoff = options.pop('cc_cutoff')
        results = {}

        # generate the command
        cmd = [ self.executable ]
        for k, v in options.items():
            cmd.append('--' + k)
            cmd.append(str(v))
        cmd.append(gctf_input)

        # log the command executed
        self.logger.info('>>> '+' '.join(map(str, cmd)))

        # execute the command
        for i in range(trials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # timeout only works if shell=False (default)
            try:
                process.wait(timeout=timeout)
                out, err = process.communicate()

                if err:
                    self.logger.warning('Gctf for micrograph {} did not finish successfully. (trial {})'.format(micrograph.basename,i+1))

                else:
                    self.logger.info('Gctf for micrograph {} was executed successfully. (trial {})'.format(micrograph.basename,i+1))

                    micrograph.files['gctf_ctf_fit'] = os.path.splitext(gctf_input)[0] + '.ctf'
                    micrograph.files['gctf_log'] = os.path.splitext(gctf_input)[0] + '_gctf.log'
                    micrograph.files['gctf_epa_log'] = os.path.splitext(gctf_input)[0] + '_EPA.log'

                    log = out.decode('utf-8')
                    with open(micrograph.files['gctf_log'], "w") as gctf_log:
                        gctf_log.write(log)

                    # find out the results of the last iteration
                    values = []
                    keys = []
                    for line in reversed(log.split('\n')):
                        # if final values not found yet:
                        if not bool(values):
                            if line.endswith('Final Values'):
                                # values as a string list
                                values = line.split()
                                # exclude 'Final' and 'Values' from list
                                values = values[:-2]
                                # convert to list of float
                                values = list(map(float, values))
                            else:
                                continue

                        # now we found the values,
                        # next line bust be the keys
                        elif bool(values):
                            keys = line.split()
                            break

                    results.update(dict(zip(keys, values)))
                    results['Defocus'] = (results['Defocus_U'] + results['Defocus_V']) / 2 / 10000
                    results['delta_Defocus'] = (results['Defocus_U'] - results['Defocus_V']) / 10000
                    if 'Phase_shift' in results:
                        results['Phase_shift'] = results['Phase_shift'] / 180

                    # Read the epa.log file into a DataFrame
                    # FIXME: first row misses!
                    epa_df = pd.read_csv(micrograph.files['gctf_epa_log'], sep='\s+',
                                         names=['Resolution', '|CTFsim|', 'EPA( Ln|F| )', 'EPA(Ln|F| - Bg)',
                                                'CCC'],
                                         header=1)

                    # find out the resolution at which the cross correlation beneath the cc_cutoff
                    index = epa_df.CCC.lt(cc_cutoff).idxmax()
                    res = epa_df.iloc[index]['Resolution']
                    results['Resolution'] = res
                    del results['CCC']  # we don't need this column anymore

                    # Read contents of the gctf star file
                    # FIXME: columns have the form '_rlnMicrographName #1', '_rlnCtfImage #2' .. KEEP IT!
                    self.logger.debug('Reading Gctf star file for micrograph {}'.format(micrograph.basename))
                    with open(ctfstar, 'r') as star_file:
                        content = star_file.read()
                        lines = list(filter(None, content.split('\n'))) #remove blank lines
                        columns = list(filter(lambda s: s.startswith('_'), lines))
                        object = lines[-1].split()
                        results.update(dict(zip(columns, object)))

                    # Delete gctf star file, we don't need this anymore
                    self.logger.debug('Removing Gctf star file {}'.format(ctfstar))
                    os.remove(ctfstar)

                    # add results to the rest of the data
                    micrograph.add_data(results)

                    # convert mrc to png. The ctf image will have the same name as the micrograph,
                    # but it is inside static/gctf, so we know what it is
                    crop_image(micrograph.files['gctf_ctf_fit'], self.static_dir, equalize_hist=False)

                    # move the log file to the static dir
                    shutil.move(micrograph.files['gctf_log'], self.static_dir)
                    micrograph.add_data(
                        {
                            'gctf_ctf_fit': 'static/gctf/{}.png'.format(micrograph.basename),
                            'gctf_log': 'static/gctf/{}'.format(os.path.basename(micrograph.files['gctf_log']))
                        }
                    )
                    return

            except subprocess.TimeoutExpired:
                self.logger.warning('Timeout of {} s expired for gctf on micrograph {}. (trial {})'.format(timeout,micrograph.basename,i+1))
                continue

        self.logger.error('Could not process gctf for micrograph {}'.format(micrograph.basename))
        return

class Motioncor:
    def __init__(self, logger, options, output_directory, executable):
        self.logger = logger
        self.options = options
        self.output_dir = output_directory
        self.results_dir = os.path.join(self.output_dir, 'motioncor')
        if not os.path.isdir(self.results_dir):
            os.makedirs(self.results_dir)
        self.static_dir = os.path.join(self.output_dir, 'static', 'motioncor') # directory to which png files will be saved to
        if not os.path.isdir(self.static_dir):
            os.makedirs(self.static_dir)
        self.executable = executable

    def __call__(self, micrograph, gpu_id):
        assert 'motioncor_input' in micrograph.files, "No motioncor input file found for micrograph {}".format(micrograph.basename)

        # set options
        options = self.options.copy() # create a copy of the dict, or other threads might override values
        options['Gpu'] = gpu_id
        options['OutMrc'] = os.path.splitext(micrograph.abspath)[0] + '.mrc'
        timeout = options.pop('timeout')
        trials = options.pop('trials')

        cmd = [self.executable]

        # convert options to a list of strings and append it to the command list
        for key, val in options.items():
            if key == 'InTiff' or key == 'InMrc':
                val = micrograph.files['motioncor_input']
            cmd.append('-' + key)
            if type(val) == list:
                val = ' '.join(map(str, val))
                cmd.append(val)
            else:
                cmd.append(str(val))

        self.logger.info('>>> ' + ' '.join(map(str, cmd)))
        for i in range(trials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=timeout)
                out, err = process.communicate()

                if err:
                    self.logger.warning('Motioncor for micrograph {name} did not finish successfully. (trial {i})\n'
                                        '{err}'.format(name = micrograph.basename,i=i+1,err=err.decode('utf-8')))
                    continue

                else:
                    self.logger.info('Motioncor for micrograph {} was executed successfully. (trial {})'.format(micrograph.basename,i+1))

                    micrograph.files['motioncor_aligned_DW'] = os.path.splitext(micrograph.abspath)[0] + '_DW.mrc'
                    micrograph.files['motioncor_aligned_no_DW'] = os.path.splitext(micrograph.abspath)[0] + '.mrc'
                    micrograph.files['motioncor_log'] = os.path.splitext(micrograph.abspath)[0] + '_DriftCorr.log'


                    with open(micrograph.files['motioncor_log'], "w") as log:
                        log.write(out.decode('utf-8'))

                    crop_image(micrograph.files['motioncor_aligned_DW'], self.static_dir, equalize_hist=True)

                    # move the log file to the static directory
                    shutil.move(micrograph.files['motioncor_log'], self.static_dir)
                    micrograph.add_data(
                        {
                            'motioncor_aligned_DW': 'static/motioncor/{}_DW.png'.format(micrograph.basename),
                            'motioncor_log': 'static/motioncor/{}'.format(os.path.basename(micrograph.files['motioncor_log']))
                        }
                    )

                    # move the other files to the results directory
                    shutil.move(micrograph.files['motioncor_aligned_DW'], self.results_dir)
                    shutil.move(micrograph.files['motioncor_aligned_no_DW'], self.results_dir)
                    micrograph.files['gctf_input'] = os.path.join(self.results_dir, os.path.basename(micrograph.files['motioncor_aligned_no_DW']))
                    return

            except subprocess.TimeoutExpired:
                self.logger.warning('Timeout of {} s expired for motioncor on micrograph {}. (trial {})'.format(timeout,micrograph.basename,i+1))
                continue

        self.logger.error("No motioncor results could be generated for micrograph {}".format(micrograph.basename))

class Micrograph:
    counter = 0
    def __init__(self, path, logger):
        self.id = Micrograph.counter
        Micrograph.counter += 1
        self.basename = os.path.splitext(os.path.basename(path))[0]
        self.abspath = os.path.abspath(path)
        self.files = {
            'raw': self.abspath,
            'motioncor_input': self.abspath,
        }
        self.data = pd.Series(name=self.id, data={'micrograph': self.basename})
        self.logger = logger

    def add_data(self, dictionary):
        self.data = pd.concat([self.data, pd.Series(data=dictionary)])
        self.data.name = self.id

def crop_image(input_mrc, output_dir, equalize_hist=False):
    """
    Converts mrc to png and saves the image inside the output directory
    basename.ext will be saved as basename.png
    :param input_mrc:
    :param output_dir:
    :param equalize_hist:
    :return:
    """
    logging.captureWarnings(True)

    with mrcfile.open(input_mrc, mode='r+', permissive=True) as mrc:
        mrc.header.map = mrcfile.constants.MAP_ID  # output .mrc files from motioncor need this correction
        mrc.update_header_from_data()
        image_ary = np.squeeze(mrc.data)  # remove single-dimensional entries
        if equalize_hist == True:
            image_ary = exposure.equalize_hist(image_ary)
        base = os.path.splitext(os.path.basename(input_mrc))[0]
        output_image = os.path.join(output_dir, base + '.png')
        misc.imsave(output_image, image_ary)

if __name__=='__main__':
    Program =  QtWidgets.QApplication(sys.argv)
    MyGui = MPIApp()
    MyGui.show()
    sys.exit(Program.exec_())



