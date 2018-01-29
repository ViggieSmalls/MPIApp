import os
import sys
from PyQt5 import QtCore, QtGui, QtWidgets
from gui import Ui_Dialog
import logging
import pyinotify
import subprocess
import shutil
from datetime import datetime
import numpy as np
import pandas as pd
from queue import Queue
from threading import Thread, Lock

import mrcfile
from scipy import misc
from skimage import exposure

class MPIApp(QtWidgets.QDialog):
    def __init__(self):
        super().__init__()
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)
        self.ui.btnInputDir.clicked.connect(self.select_input_directory)
        self.ui.btnOutputDir.clicked.connect(self.select_output_directory)
        self.ui.btnGain.clicked.connect(self.select_gain)
        self.ui.btn_Run.clicked.connect(self.accept)
        self.ui.btn_Exit.clicked.connect(self.exit)

        self.main_defaults = {
            'InputDir': os.path.abspath('.'),
            'OutputDir': os.path.join(os.path.abspath('.'), 'output'),
            'Gain': 'select a file',
            'kV': 300,
            'apix': 1.000,
            'dose_per_frame': 1.000,
            'cs': 2.62,
            'ac': 0.1
        }
        self.set_main_defaults()

        self.motioncor_defaults = {
            'Serial': 0,
            'MaskCent': (0.0000, 0.0000),
            'MaskSize': (1.0000, 1.0000),
            'Patch': (0, 0),
            'Iter': 7,
            'Tol': 0.5000,
            'Bft': 100.000,
            'StackZ': 0,
            'FtBin': 1.0000,
            'InitDose': 0.0000,
            'FmDose': 0.0000,
            'PixSize': 0.0000,
            'kV': 300,
            'Throw': 0,
            'Trunc': 0,
            'Group': 1,
            'FmRef': -1,
            'OutStack': 0,
            'RotGain': 0,
            'FlipGain': 0,
            'Align': 1,
            'Tilt': (0.0000, 0.0000),
            'Mag': (1.0000, 1.0000, 0.0000),
            'Crop': (0, 0),
            'Gpu': 0
        }
        self.set_motioncor_defaults()

        self.gctf_defaults = {
            'apix', 'kV', 'cs', 'ac', 'phase_shift_L', 'phase_shift_H', 'phase_shift_S', 'phase_shift_T', 'dstep',
        'defL', 'defH', 'defS', 'astm', 'bfac', 'resL', 'resH', 'boxsize', 'do_EPA', 'EPA_oversmp', 'overlap',
        'convsize', 'do_Hres_ref', 'Href_resL', 'Href_resH', 'Href_bfac', 'B_resL', 'B_resH', 'do_mdef_refine',
        'mdef_aveN', 'mdef_fit', 'mdef_ave_type', 'do_local_refine', 'local_radius', 'local_avetype', 'local_boxsize',
        'local_overlap', 'local_resL', 'local_resH', 'refine_local_astm', 'refine_input_ctf', 'defU_init', 'defV_init',
        'defA_init', 'B_init', 'defU_err', 'defV_err', 'defA_err', 'B_err', 'do_phase_flip', 'do_validation',
        'ctfout_resL', 'ctfout_resH', 'ctfout_bfac', 'input_ctfstar', 'boxsuffix', 'logsuffix',
        'write_local_ctf', 'plot_res_ring', 'do_unfinished', 'skip_check_mrc', 'skip_check_gpu', 'estimate_B',
        'refine_after_EPA'
        }

        self.process_table = pd.DataFrame()
        self.process_table_lock = Lock()

    def select_input_directory(self):
        directory = str(QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory"))
        self.ui.line_InputDir.setText(directory)

    def select_output_directory(self):
        directory = str(QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory"))
        self.ui.line_OutputDir.setText(directory)

    def select_gain(self):
        file = str(QtWidgets.QFileDialog.getOpenFileName(self, "Select File")[0])
        self.ui.line_Gain.setText(file)

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
            self.motioncor_defaults['InMrc'] = '{motioncor_input}'
        elif self.ui.radio_tif.isChecked():
            self.file_extension = '.tif'
            self.motioncor_defaults['InTiff'] = '{motioncor_input}'
        if not hasattr(self, 'file_extension'):
            raise ValueError('No file extension selected')

    def get_input_dir(self):
        self.inputDir = self.ui.line_InputDir.text()
        if not os.path.isdir(self.inputDir):
            raise ValueError('The input directory does not exist')

    def get_output_dir(self):
        """
        creates at maximum one subfoder to an existing directory
        """
        self.outputDir = self.ui.line_OutputDir.text()
        if not os.path.isdir(self.outputDir):
            try:
                os.mkdir(self.outputDir)
            except Exception as ex:
                raise type(ex)(str(ex) + '(Output Directory)')

    def set_main_defaults(self):
        """
        Sets the default line entries for the Main Tab
        """
        for param in self.main_defaults.keys():
            line = getattr(self.ui, 'line_{}'.format(param))
            value = self.main_defaults[param]
            text = str(value)
            line.setText(text)
        self.ui.radio_tif.setChecked(True)

    def set_motioncor_defaults(self):
        """
        Sets the placeholder text for the line inputs in the Motioncor Tab
        """
        for param in self.motioncor_defaults.keys():
            try:
                line = getattr(self.ui, 'motioncor_{}'.format(param))
                value = self.motioncor_defaults[param]
                if type(value) == tuple:
                    placeholder = ' '.join(map(str, value))
                else:
                    placeholder = str(value)
                line.setPlaceholderText(placeholder)
            except:
                pass # there is no such line attribute

    def set_parameter(self, dict, key, param_as_text):
        type_ref = type(dict[key])
        try:
            if type_ref == tuple:
                type_item = type(dict[key][0])
                dict[key] = tuple(map(type_item, param_as_text.split()))
            else:
                dict[key] = type_ref(param_as_text)
        except Exception as ex:
            raise type(ex)(str(ex) + key)

    def get_motioncor_options(self):
        try:
            self.set_parameter(self.motioncor_defaults,'kV', self.ui.line_kV.text())
            self.set_parameter(self.motioncor_defaults,'PixSize', self.ui.line_apix.text())
            self.set_parameter(self.motioncor_defaults,'FmDose', self.ui.line_dose_per_frame.text())
            self.set_parameter(self.main_defaults,'Gain', self.ui.line_Gain.text())
            self.motioncor_defaults['Gain'] = self.main_defaults['Gain']
            self.motioncor_timeout = int(self.ui.motioncor_timeout.text())
            self.motioncor_trials = int(self.ui.motioncor_trials.text())
        except Exception as ex:
            raise ex

    def get_gctf_options(self):
        try:
            self.set_parameter(self.gctf_defaults, 'kV', self.ui.line_kV.text())
            self.set_parameter(self.gctf_defaults, 'apix', self.ui.line_apix.text())
            self.set_parameter(self.gctf_defaults, 'cs', self.ui.line_cs.text())
            self.set_parameter(self.gctf_defaults, 'ac', self.ui.line_ac.text())
            self.gctf_timeout = 100
            self.gctf_trials = 3
            self.gctf_cc_cutoff = 0.75
        except Exception as ex:
            raise ex

    def start_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(os.path.join(self.outputDir, 'mpiapp.log'))
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def start_process_queue(self):
        self.queue = Queue()

    def start_event_notifier(self):
        wm = pyinotify.WatchManager()
        self.notifier = pyinotify.ThreadedNotifier(wm, EventHandler(logger=self.logger, queue=self.queue, file_extension=self.file_extension))
        self.notifier.daemon = True
        self.notifier.start()
        wm.add_watch(self.inputDir, pyinotify.ALL_EVENTS)

    def worker(self, gpu_id):
        motioncor = Motioncor(self.motioncor_timeout, self.motioncor_trials, self.logger, self.motioncor_defaults,
                                   self.outputDir)
        gctf = Gctf(self.gctf_timeout, self.gctf_trials, self.logger, self.gctf_defaults, self.outputDir, self.gctf_cc_cutoff)
        while True:
            micrograph = self.queue.get()
            self.logger.info('Processing Micrograph {}'.format(micrograph.basename))
            try:
                motioncor(micrograph, gpu_id)
                self.process_table.update(micrograph)
                gctf(micrograph, gpu_id)
                self.process_table_update(micrograph)
            except Exception as ex:
                self.logger.error(str(ex))

    def start_worker_threads(self):
        gpu_threads = [Thread(target=self.worker, args=(i,)) for i in self.GPUs]
        for thread in gpu_threads:
            self.logger.info('Starting thread for GPU with ID: {}'.format(thread._args[0]))
            thread.daemon = True
            thread.start()

    def process_table_update(self, micrograph):
        """
        Update the table with the micrograph data. This replaces
        previous values, but can also add new columns to the
        DataFrame
        :param micrograph:
        """
        self.process_table_lock.acquire()
        self.df.loc[micrograph.id] = micrograph.data
        self.process_table_lock.release()

    def process_table_dump(self):

        csv_file = "process_table.csv"
        gctf_star = 'micrographs_all_gctf.star'

        self.process_table_lock.acquire()

        # write to process_table.csv
        if not self.df.empty:
            self.df['Defocus'] = self.df[["Defocus_U", "Defocus_V"]].mean(axis=1)
            self.df[['Defocus', 'Defocus_U', 'Defocus_V']] = self.df[['Defocus', 'Defocus_U', 'Defocus_V']] / 1000
            self.df[['Phase_shift']] = self.df[['Phase_shift']] / 180
            self.df['delta_Defocus'] = self.df["Defocus_U"] - self.df["Defocus_V"]

        self.df.to_csv(csv_file, index_label='micrograph')

        # write to micrographs_all_gctf.star
        if not self.df.empty:
            _rln = self.df.filter(regex=("^_rln.*"))
            keys = list(_rln.columns)
            d = [i.split() for i in keys]
            d = [(i[0], int(i[1][1:])) for i in d]
            sorted_list = sorted(d, key=lambda x: x[1])
            columns = [i[0] + ' #' + str(i[1]) for i in sorted_list]
            rln = _rln[columns]
            rln['_rlnMicrographName #1'] = self.df['motioncor_aligned_no_DW']
            rln['_rlnCtfImage #2'] = self.df['gctf_ctf_fit'].apply(lambda s: s + ':mrc')
            rln.to_csv(gctf_star, index=False, header=False, sep='\t')

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
            self.get_motioncor_options()
            self.ui.btn_Run.setText('Abort')
            self.ui.btn_Run.clicked.connect(self.abort)
            self.run()
        except Exception as ex:
            QtWidgets.QMessageBox.about(self, 'ERROR',str(ex))

    def run(self):
        self.start_logging()
        self.start_process_queue()
        self.start_event_notifier()
        self.start_worker_threads()
        pass

    def abort(self):
        sys.exit()
        pass

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
    def __init__(self, timeout, trials, logger, options, output_directory, cc_cutoff):
        self.timeout = timeout
        self.trials = trials
        self.logger = logger
        self.options = options
        self.output_dir = output_directory
        self.static_dir = os.path.join(self.output_dir, 'static', 'gctf') # directory to which png files will be saved to
        self.cc_cutoff = cc_cutoff

    def __call__(self, micrograph, gpu_id):
        assert 'gctf_input' in micrograph.files
        options = self.options.copy()
        options['gid'] = gpu_id
        ctfstar = os.path.join(micrograph.basename + '.star')
        options['ctfstar'] = ctfstar
        results = {}

        cmd = [ 'gctf' ]
        for k, v in options.items():
            cmd.append('--' + k)
            cmd.append(str(v))

        cmd.append(micrograph.files['gctf_input'])
        self.logger.info('>>> '+' '.join(map(str, cmd)))

        for i in range(self.trials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=self.timeout)
                out, err = process.communicate()

                if err:
                    self.logger.warning('Gctf for micrograph {} did not finish successfully. (trial {})\n'.format(micrograph.basename,i+1))

                else:
                    self.logger.info('Gctf for micrograph {} was executed successfully. (trial {})\n'.format(micrograph.basename,i+1))

                    micrograph.files['gctf_ctf_fit'] = os.path.splitext(micrograph.abspath)[0] + '.ctf'
                    micrograph.files['gctf_log'] = os.path.splitext(micrograph.abspath)[0] + '_gctf.log'
                    micrograph.files['gctf_epa_log'] = os.path.splitext(micrograph.abspath)[0] + '_EPA.log'

                    log = out.decode('utf-8')
                    with open(micrograph.files['gctf_log'], "w") as gctf_log:
                        gctf_log.write(log)

                    for line in reversed(log.split('\n')):
                        if line.endswith('Final Values'):
                            data = np.array(line.split()[:-2]).astype(float)
                            #FIXME: what if gctf does not correct with phase shift?
                            keys = ['Defocus_U', 'Defocus_V', 'Angle', 'Phase_shift', 'CCC']
                            results.update(dict(zip(keys, list(data))))
                            break

                    # FIXME: first row misses!
                    epa_df = pd.read_csv(micrograph.files['gctf_epa_log'], sep='\s+',
                                         names=['Resolution', '|CTFsim|', 'EPA( Ln|F| )', 'EPA(Ln|F| - Bg)',
                                                'CCC'],
                                         header=1)

                    index = epa_df.CCC.lt(self.cc_cutoff).idxmax()
                    res = epa_df.iloc[index]['Resolution']
                    results['Resolution'] = res
                    del results['CCC'] # we don't need this column

                    self.logger.debug('Reading Gctf star file for micrograph {}'.format(micrograph.basename))
                    with open(ctfstar, 'r') as star_file:
                        content = star_file.read()
                        lines = list(filter(None, content.split('\n')))
                        columns = list(filter(lambda s: s.startswith('_'), lines))
                        object = lines[-1].split()
                        results.update(dict(zip(columns, object)))

                    self.logger.debug('Removing Gctf star file {}'.format(ctfstar))
                    os.remove(ctfstar)

                    micrograph.add_data(results)
                    return

            except subprocess.TimeoutExpired:
                self.logger.warning('Timeout of {} s expired for gctf on micrograph {}. (trial {})\n'.format(self.timeout,micrograph.basename,i+1))
                continue

        self.logger.error('Could not process gctf for micrograph {}'.format(micrograph.basename))
        return

class Motioncor:
    def __init__(self, timeout, trials, logger, options, output_directory):
        self.timeout = timeout
        self.trials = trials
        self.logger = logger
        self.options = options
        self.output_dir = output_directory
        self.static_dir = os.path.join(self.output_dir, 'static', 'motioncor') # directory to which png files will be saved to

    def __call__(self, micrograph, gpu_id):
        assert 'motioncor_input' in micrograph.files
        options = self.options.copy() # create a copy of the dict, or other threads might override values
        options['Gpu'] = gpu_id

        cmd = ['motioncor']

        # convert options to a list of strings and append it to the executable
        for key, val in options.items():
            if key == 'InTiff' or key == 'InMrc':
                val = micrograph.files['motioncor_input']
            cmd.append('-' + key)
            if type(val) == tuple:
                val = ' '.join(map(str, val))
                cmd.append(val)
            else:
                cmd.append(str(val))

        self.logger.info('>>> ' + ' '.join(map(str, cmd)))
        for i in range(self.trials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=self.timeout)
                out, err = process.communicate()

                if err:
                    self.logger.warning('Motioncor for micrograph {} did not finish successfully. (trial {})\n'.format(micrograph.basename,i+1))
                    continue

                else:
                    self.logger.info('Motioncor for micrograph {} was executed successfully. (trial {})\n'.format(micrograph.basename,i+1))

                    micrograph.files['motioncor_aligned_DW'] = os.path.splitext(micrograph.abspath)[0] + '_DW.mrc'
                    micrograph.files['motioncor_aligned_no_DW'] = os.path.splitext(micrograph.abspath)[0] + '.mrc'
                    micrograph.files['motioncor_log'] = os.path.splitext(micrograph.abspath)[0] + '_DriftCorr.log'
                    micrograph.files['gctf_input'] = micrograph.files['motioncor_aligned_no_DW']

                    with open(micrograph.files['motioncor_log'], "w") as log:
                        log.write(out.decode('utf-8'))

                    crop_image(micrograph.files['motioncor_aligned_DW'], self.static_dir, equalize_hist=True)
                    micrograph.add_data({'motioncor_aligned_DW': 'static/motioncor/{}_DW.png'.format(micrograph.basename)})
                    return

            except subprocess.TimeoutExpired:
                self.logger.warning('Timeout of {} s expired for motioncor on micrograph {}. (trial {})\n'.format(self.timeout,micrograph.basename,i+1))
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
            'motioncor_input': self.abspath,
        }
        self.data = pd.Series()
        self.logger = logger

    def add_data(self, dictionary):
        self.data = pd.concat([self.data, pd.Series(data=dictionary)])

def crop_image(input_mrc, output_dir, equalize_hist=False):
    logging.captureWarnings(True)

    with mrcfile.mmap(input_mrc, mode='r+', permissive=True) as mrc:
        mrc.header.map = mrcfile.constants.MAP_ID  # output .mrc files from motioncor need this correction
        mrc.update_header_from_data()

    mrc = mrcfile.open(input_mrc)
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



