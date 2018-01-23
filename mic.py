import os
import shutil
import numpy as np
import pandas as pd
from threading import Lock
import configparser

class File:
    def __init__(self, full_path):
        self._directory = os.path.dirname(full_path)
        self.name = os.path.basename(full_path)
        self._abspath = full_path

    @property
    def directory(self):
        return self._directory

    @directory.setter
    def directory(self, new_path):
        assert os.path.isdir(new_path)
        self._directory = new_path

    @property
    def abspath(self):
        return os.path.join(self.directory, self.name)

    @abspath.setter
    def abspath(self, new_abspath):
        dir = os.path.dirname(new_abspath)
        name = os.path.basename(new_abspath)
        self.directory = dir
        self.name = name

    def move_to_directory(self, directory):
        assert os.path.isdir(directory)
        shutil.move(self.abspath, os.path.join(directory, self.name))
        self.directory = directory

    def __str__(self):
        return self.abspath

    def __repr__(self):
        return self.abspath

class Micrograph:
    counter = 0
    def __init__(self, basename, path):
        self.id = Micrograph.counter
        Micrograph.counter += 1
        self.basename = basename
        self.files = {
            'raw': File(path)
        }
        # create the process directory

def execution_successful(command, timeout, trials):
    pass


class Motioncor:
    def __init__(self, options):
        """
        conf = RawConfigParser()
        conf.optionxform = lambda option: option
        conf.read('/home/victor/PycharmProjects/mpiapp/tests/conf.new')
        :param options: conf['motioncor']
        """
        self.conf = options
        self.executable = self.conf['executable']
        self.timeout = self.conf['timeout']
        self.trials = self.conf['trials']
        self.valid_parameters = ['InTiff', 'OutMrc', 'Gain']

    def process(self, micrograph):
        input = micrograph.files['raw']
        self.conf['input'] = input
        if 'OutMrc' in self.conf.keys():
            self.conf['output'] = os.path.join(input.directory, micrograph.basename + '.mrc')
        elif 'OutMrcs' in self.conf.keys():
            self.conf['output'] = os.path.join(input.directory, micrograph.basename + '.mrcs')

        parameters = {key: self.conf[key] for key in self.valid_parameters}
        cmd = [self.executable]
        for parameter,value in parameters.items():
            cmd.extend(['-' + parameter, str(value)])

        if execution_successful(cmd, self.timeout, self.trials) == True:
            #FIXME: filepaths
            micrograph.files['motioncor_aligned_DW'] = File(os.path.join(input.directory, micrograph.basename + '_DW.mrc'))
            micrograph.files['motioncor_aligned_DW_png'] = File(os.path.join(input.directory, micrograph.basename + '_DW.png'))
            micrograph.files['motioncor_aligned_no_DW'] = File(os.path.join(input.directory, micrograph.basename + '.mrc'))
            micrograph.files['motioncor_aligned_no_DW_png'] = File(os.path.join(input.directory, micrograph.basename + '.png'))
            micrograph.files['motioncor_log'] = File(os.path.join(input.directory, micrograph.basename + '_DriftCorr.log'))
        else:
            pass
        # parse the options that have valid parameters
        # create a command that can be called by subprocess.Popen
        # execute(command, timeout, trials)
        # return True if micrograph could be processed
            # add to micrograph files all produced output files
            # postprocess
            # update the process table with the new columns and results
        # return False if micrograph could not be processed

        pass

class Gctf:
    def __init__(self, options):
        self.timeout = options['timeout']
        self.ntrials = options['trials']
        self.cc_cutoff = options['cc_cutoff']

class ProcessTable:
    def __init__(self):
        self.df = pd.DataFrame()
        self.lock = Lock()
        self.file = "process_table.csv"
        self.gctf_star = 'micrographs_all_gctf.star'

    def update(self, micrograph, values_as_dict):
        self.lock.acquire()
        self.df = pd.concat([self.df, pd.DataFrame(data=values_as_dict, index=[micrograph.id])])
        self.lock.release()

    def dump(self):
        self.lock.acquire()

        # write to process_table.csv
        if not self.df.empty:
            self.df['Defocus'] = self.df[["Defocus_U", "Defocus_V"]].mean(axis=1)
            self.df[['Defocus', 'Defocus_U', 'Defocus_V']] = self.df[['Defocus', 'Defocus_U', 'Defocus_V']] / 1000
            self.df[['Phase_shift']] = self.df[['Phase_shift']] / 180
            self.df['delta_Defocus'] = self.df["Defocus_U"] - self.df["Defocus_V"]

        #TODO: füge die spalte micrograph hinzu, sonst gibt es probleme mit der website
        self.df.to_csv(self.file, index_label='micrograph')

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
            rln['_rlnCtfImage #2'] = self.df['gctf_ctf_fit'].apply(lambda s: s+':mrc')
            rln.to_csv(self.gctf_star, index=False, header=False, sep='\t')

            with open(self.gctf_star, 'r+') as f:
                content = f.read()
                f.seek(0, 0)
                f.write('data_\nloop_\n' + '\n'.join(columns) + '\n' + content)

        self.lock.release()


