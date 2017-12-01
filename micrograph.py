import os
import shutil
import subprocess
import numpy as np
import pandas as pd
from datetime import datetime

class File:
    def __init__(self, full_path):
        self._path = os.path.dirname(full_path)
        self.name = os.path.basename((full_path))
        self._abspath = full_path

    # FIXME rename to dir
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

class Micrograph:
    def __init__(self, name):
        self.micrograph = File(name)
        self.dir = os.path.dirname(self.micrograph)
        self.name = os.path.basename(self.micrograph)
        self.basename, self.extension = os.path.splitext(self.name)
        self._motioncor_options = None
        self._gctf_options = None
        self.created_at = datetime.now()
        self.ntrials = 3
        self.timeout = 300 # seconds
        #FIXME: Write to log
        print('Created Micrograph class for {}'.format(self.name))

    @property
    def motioncor_options(self):
        return self._motioncor_options

    @motioncor_options.setter
    def motioncor_options(self, options):
        try:
            self._motioncor_options = options
            self.motioncor_executable = self._motioncor_options.pop('path_to_executable')
        except:
            # FIXME write to log
            print("Motioncor options not valid.")

    @property
    def gctf_options(self):
        return self._gctf_options

    @gctf_options.setter
    def gctf_options(self, options):
        try:
            self._gctf_options = options
            self.gctf_executable = self._gctf_options.pop('path_to_executable')
            self.gctf_results = None
            self.gctf_output_files = []
        except:
            # FIXME write to log
            print("Gctf options not valid.")

    def process(self, gpu_id):
        self.process_dir = os.path.join(self.dir, self.basename)
        os.mkdir(self.process_dir)
        self.micrograph.move_to_directory(self.process_dir)
        os.chdir(self.process_dir)

        if not self._motioncor_options:
            #FIXME: write to log
            print('No motioncor parameters provided. Skipping motioncor for micrograph {}'.format(self.name))
        else:
            self.run_motioncor(gpu_id)
        if not self._gctf_options:
            #FIXME: write to log
            print('No gctf parameters provided. Skipping gctf for micrograph {}'.format(self.name))
        else:
            self.run_gctf(gpu_id)

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
        # FIXME: Log cmd to log file
        print(' '.join(map(str, cmd)))
        # TODO: return result as a dictionary, or 'failed' as values of dictionary
        for i in range(self.ntrials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=self.timeout)
                out, err = process.communicate()

                if err:
                    print('Motioncor for micrograph {} did not finish successfully. (trial {})\n'.format(self.name,i+1))

                else:
                    print('Motioncor for micrograph {} was executed successfully. (trial {})\n'.format(self.name,i+1))
                    self.motioncor_output_files = {
                        'motioncor_aligned_DW': File(os.path.splitext(self.micrograph)[0] + '_DW.mrc'),
                        'motioncor_aligned_no_DW': File(os.path.splitext(self.micrograph)[0] + '.mrc'),
                        'motioncor_log': File(os.path.splitext(self.micrograph)[0] + '_DriftCorr.log')
                    }
                    with open(self.motioncor_output_files['motioncor_log'].abspath, "w") as log:
                        log.write(out.decode('utf-8'))
                    # output a dictionary with the names of the output files
                    # TODO add location of files
                    self.motioncor_results = {k:v.name for k,v in self.motioncor_output_files.items()}
                    return

            except subprocess.TimeoutExpired:
                print('Timeout of {} s expired for motioncor on micrograph {}. (trial {})\n'.format(self.timeout,self.name,i+1))
                continue

        self.motioncor_results = {}
        return

    def run_gctf(self, gpu_id):
        try:
            self.gctf_input = self.motioncor_output_files['aligned_no_DW']
        except:
            # FIXME write to log
            print('No input for gctf available. Skipping gctf for micrograph {}'.format(self.name))
            return

        cmd = [ self.gctf_executable ]
        for k, v in self.gctf_options.items():
            # FIXME what happens if gpu id option is accidentally given in the config file?
            try: v = v.format(gpu_id=gpu_id)
            except: pass
            finally:
                k = '--' + k
                cmd.extend([k,str(v)])
        cmd.append(self.gctf_input)
        # FIXME: Log cmd to log file
        print(' '.join(map(str, cmd)))

        for i in range(self.ntrials):
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                process.wait(timeout=self.timeout)
                out, err = process.communicate()

                if err:
                    print('Gctf for micrograph {} did not finish successfully. (trial {})\n'.format(self.name,i+1))

                else:
                    print('Gctf for micrograph {} was executed successfully. (trial {})\n'.format(self.name,i+1))

                    self.gctf_output_files = {
                        'gctf_power_spectrum': File(os.path.splitext(self.gctf_input)[0] + '.ctf'),
                        'gctf_log': File(os.path.splitext(self.gctf_input)[0] + '_gctf.log'),
                        'gctf_epa_log': File(os.path.splitext(self.gctf_input)[0] + '_EPA.log')
                    }
                    log = out.decode('utf-8')
                    with open(self.gctf_output_files['gctf_log'].abspath, "w") as logfile:
                        logfile.write(log)

                    for line in reversed(log.split('\n')):
                        if line.endswith('Final Values'):
                            data = np.array(line.split()[:-2]).astype(float)
                            keys = ['Defocus_U', 'Defocus_V', 'Angle', 'Phase_shift', 'CCC']
                            self.gctf_results = dict(zip(keys, list(data)))
                            break

                    # FIXME: first row misses!
                    epa_df = pd.read_csv(self.gctf_output_files['epa_log'].abspath, sep='\s+',
                                         names=['Resolution', '|CTFsim|', 'EPA( Ln|F| )', 'EPA(Ln|F| - Bg)',
                                                'CCC'],
                                         header=1)
                    cut_off_ccc = 0.75
                    index = epa_df.CCC.lt(cut_off_ccc).idxmax()
                    res = epa_df.iloc[index]['Resolution']
                    self.gctf_results['Resolution'] = res
                    del self.gctf_results['CCC']

                    # TODO add location of files
                    self.gctf_results.update( {k:v.name for k,v in self.motioncor_output_files.items()} )
                    return

            except subprocess.TimeoutExpired:
                print('Timeout of {} s expired for motioncor on micrograph {}. (trial {})\n'.format(self.timeout,self.name,i+1))
                continue

        self.gctf_results = {}
        return

    def move_to_output_directory(self, directory):
        # FIXME print to log
        assert os.path.isdir(directory), print('Target directory does not exist!')
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
