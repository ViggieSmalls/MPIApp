from configobj import ConfigObj
import os

class ConfigParser(ConfigObj):
    #TODO check if options are valid
    # FIXME what happens if gpu id option is accidentally given in the config file?

    def __init__(self, config_file):
        ConfigObj.__init__(self, config_file)
        self.setup_options = self['setup']
        self.general_options = self['general']
        self.motioncor_options = self['motioncor']
        self.gctf_options = self['gctf']

        self.input_directory = self.setup_options['input_directory']
        self.output_directory = self.setup_options['output_directory']
        self.static_directory = os.path.join(self.output_directory, 'static_files')
        self.GPUs = list(map(int, self.setup_options['GPUs'].split()))
        self.file_extesion = self.setup_options['file_extension']
        self.logfile = os.path.join(self.output_directory, self.setup_options['logfile'])

        self.pixel_size = float(self.general_options['pixel_size'])
        self.voltage = float(self.general_options['voltage'])
        self.dose_per_frame = float(self.general_options['dose_per_frame'])
        self.cs = float(self.general_options['cs'])
        self.ac = float(self.general_options['amplitude_contrast'])

        self.motioncor_options.update({
            'kV': self.voltage,
            'PixSize': self.pixel_size,
            'Gpu': '{gpu_id}',
            'FmDose': self.dose_per_frame
        })

        self.gctf_options.update({
            'ac': self.ac,
            'cs': self.cs,
            'kV': self.voltage,
            'apix': self.pixel_size if 'FtBin' not in self.motioncor_options else self.pixel_size*float(self.motioncor_options['FtBin']) ,
            'gid': '{gpu_id}',
        })
