import os
import shutil
import subprocess
import numpy as np
import pandas as pd
from datetime import datetime
import logging

class File:
    def __init__(self, full_path):
        self._path = os.path.dirname(full_path)
        self.name = os.path.basename(full_path)
        self._abspath = full_path

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, new_path):
        assert os.path.isdir(new_path)
        self._path = new_path

    @property
    def abspath(self):
        return os.path.join(self.path, self.name)

    @abspath.setter
    def abspath(self, new_abspath):
        dir = os.path.dirname(new_abspath)
        name = os.path.basename(new_abspath)
        self.path = dir
        self.name = name

    def move_to_directory(self, directory):
        assert os.path.isdir(directory)
        shutil.move(self.abspath, os.path.join(directory, self.name))
        self.path = directory

    def __str__(self):
        return self.abspath

    def __repr__(self):
        return self.abspath

class Micrograph:
    def __init__(self, name):
        self.logger = logging.getLogger('mpi_application')
        self.micrograph = File(name)
        self.dir = os.path.dirname(self.micrograph.abspath)
        self.name = os.path.basename(self.micrograph.abspath)
        self.basename, self.extension = os.path.splitext(self.name)
        self._motioncor_options = None
        self._gctf_options = None
        self.created_at = datetime.now()
        self.ntrials = 3
        self.timeout = 300 # seconds
        self.logger.info('Micrograph object created for {}.'.format(self.name))

    @property
    def motioncor_options(self):
        return self._motioncor_options

    @motioncor_options.setter
    def motioncor_options(self, options):
        try:
            self._motioncor_options = options
            self.motioncor_executable = self._motioncor_options.pop('path_to_executable')
        except:
            self.logger.warning("Motioncor options are not valid")

    @property
    def gctf_options(self):
        return self._gctf_options

    @gctf_options.setter
    def gctf_options(self, options):
        try:
            self._gctf_options = options
            self.gctf_executable = self._gctf_options.pop('path_to_executable')
        except:
            self.logger.warning("Gctf options are not valid")

    def process(self, gpu_id):
        self.process_dir = os.path.join(self.dir, self.basename)
        os.mkdir(self.process_dir)
        self.micrograph.move_to_directory(self.process_dir)
        os.chdir(self.process_dir)

        if not self.motioncor_options:
            self.logger.info('No motioncor parameters provided. Skipping motioncor for micrograph {}'.format(self.name))
        else:
            self.run_motioncor(gpu_id)
            self.logger.info({self.micrograph.name:self.motioncor_results})
        if not self.gctf_options:
            self.logger.info('No gctf parameters provided. Skipping gctf for micrograph {}'.format(self.name))
        else:
            self.run_gctf(gpu_id)
            self.logger.info({self.micrograph.name:self.gctf_results})
        os.chdir('..')

    def run_motioncor(self, gpu_id):
        cmd = [ self.motioncor_executable ]
        for k, v in self.motioncor_options.items():
            try: v = v.format(basename=self.basename)
            except: pass
            try: v = v.format(gpu_id=gpu_id)
            except: pass
            finally:
                k = '-' + k
                cmd.extend([k,str(v)])

        self.logger.info('Executing: ' + ' '.join(map(str, cmd)))
        for i in range(self.ntrials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=self.timeout)
                out, err = process.communicate()

                if err:
                    self.logger.warning('Motioncor for micrograph {} did not finish successfully. (trial {})\n'.format(self.name,i+1))
                    continue

                else:
                    self.logger.info('Motioncor for micrograph {} was executed successfully. (trial {})\n'.format(self.name,i+1))
                    self.motioncor_output_files = {
                        'motioncor_aligned_DW': File(os.path.splitext(self.micrograph.abspath)[0] + '_DW.mrc'),
                        'motioncor_aligned_no_DW': File(os.path.splitext(self.micrograph.abspath)[0] + '.mrc'),
                        'motioncor_log': File(os.path.splitext(self.micrograph.abspath)[0] + '_DriftCorr.log')
                    }
                    with open(self.motioncor_output_files['motioncor_log'].abspath, "w") as log:
                        log.write(out.decode('utf-8'))
                    # output a dictionary with the names of the output files
                    self.motioncor_results = {k:os.path.join('motioncor',v.name) for k,v in self.motioncor_output_files.items()}
                    return

            except subprocess.TimeoutExpired:
                self.logger.warning('Timeout of {} s expired for motioncor on micrograph {}. (trial {})\n'.format(self.timeout,self.name,i+1))
                continue

        self.logger.error("No motioncor results could be generated for micrograph {}".format(self.micrograph.name))
        self.motioncor_results = {}
        self.motioncor_output_files = {}
        return

    def run_gctf(self, gpu_id):
        if not self.motioncor_options:
            self.gctf_input = self.micrograph
        else:
            try:
                self.gctf_input = self.motioncor_output_files['motioncor_aligned_no_DW']
                self.logger.info("Using {} as input for gctf".format(self.gctf_input.name))
            except:
                self.logger.warning('No input for gctf available. Skipping gctf for micrograph {}'.format(self.name))
                return

        # FIXME implement more nicely
        self.gctf_ctfstar = os.path.join(self.process_dir, self.basename + '.star')
        self.gctf_options['ctfstar'] = self.gctf_ctfstar

        cmd = [ self.gctf_executable ]
        for k, v in self.gctf_options.items():
            try: v = v.format(gpu_id=gpu_id)
            except: pass
            finally:
                k = '--' + k
                cmd.extend([k,str(v)])
        cmd.append(self.gctf_input.abspath)
        self.logger.info('Execute: '+' '.join(map(str, cmd)))

        for i in range(self.ntrials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=self.timeout)
                out, err = process.communicate()

                if err:
                    self.logger.warning('Gctf for micrograph {} did not finish successfully. (trial {})\n'.format(self.gctf_input.name,i+1))

                else:
                    self.logger.info('Gctf for micrograph {} was executed successfully. (trial {})\n'.format(self.gctf_input.name,i+1))

                    self.gctf_output_files = {
                        'gctf_ctf_fit': File(os.path.splitext(self.gctf_input.abspath)[0] + '.ctf'),
                        #'gctf_power_spectrum': File(os.path.splitext(self.gctf_input.abspath)[0] + '.pow'),
                        'gctf_log': File(os.path.splitext(self.gctf_input.abspath)[0] + '_gctf.log'),
                        'gctf_epa_log': File(os.path.splitext(self.gctf_input.abspath)[0] + '_EPA.log')
                    }
                    log = out.decode('utf-8')
                    with open(self.gctf_output_files['gctf_log'].abspath, "w") as gctf_log:
                        gctf_log.write(log)

                    for line in reversed(log.split('\n')):
                        if line.endswith('Final Values'):
                            data = np.array(line.split()[:-2]).astype(float)
                            keys = ['Defocus_U', 'Defocus_V', 'Angle', 'Phase_shift', 'CCC']
                            self.gctf_results = dict(zip(keys, list(data)))
                            break

                    # FIXME: first row misses!
                    epa_df = pd.read_csv(self.gctf_output_files['gctf_epa_log'].abspath, sep='\s+',
                                         names=['Resolution', '|CTFsim|', 'EPA( Ln|F| )', 'EPA(Ln|F| - Bg)',
                                                'CCC'],
                                         header=1)
                    cut_off_ccc = 0.75
                    index = epa_df.CCC.lt(cut_off_ccc).idxmax()
                    res = epa_df.iloc[index]['Resolution']
                    self.gctf_results['Resolution'] = res
                    del self.gctf_results['CCC']

                    self.gctf_results.update( {k:os.path.join('gctf',v.name) for k,v in self.gctf_output_files.items()} )
                    return

            except subprocess.TimeoutExpired:
                self.logger.warning('Timeout of {} s expired for gctf on micrograph {}. (trial {})\n'.format(self.timeout,self.name,i+1))
                continue

        self.gctf_results = {}
        self.gctf_output_files = {}
        return

    def move_to_output_directory(self, directory):
        assert os.path.isdir(directory), self.logger.error('Target directory does not exist!')
        raw_frames_dir = os.path.join(directory, 'frames')
        motioncor_dir = os.path.join(directory, 'motioncor')
        gctf_dir = os.path.join(directory, 'gctf')
        for dir in [raw_frames_dir, motioncor_dir, gctf_dir]:
            if not os.path.isdir(dir):
                os.mkdir(dir)
        self.micrograph.move_to_directory(raw_frames_dir)
        for file in self.motioncor_output_files.values():
            file.move_to_directory(motioncor_dir)
        for file in self.gctf_output_files.values():
            file.move_to_directory(gctf_dir)
