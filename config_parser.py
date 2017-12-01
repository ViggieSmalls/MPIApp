from configobj import ConfigObj
import os

class ConfigParser(ConfigObj):
    #TODO check if options are valid
    def __init__(self, config_file):
        ConfigObj.__init__(self, config_file)
        self.setup_options = self['setup']
        self.general_options = self['general']
        self.motioncor_options = self['motioncor']
        self.gctf_options = self['gctf']

        self.input_directory = self.setup_options['input directory']
        self.output_directory = self.setup_options['output directory']
        self.static_directory = os.path.join(self.output_directory, 'static_files')
        self.GPUs = list(map(int, self.setup_options['GPUs'].split()))
        self.port_number = int(self.setup_options['port number'])
        self.file_extesion = self.setup_options['file extension']

        self.pixel_size = float(self.general_options['pixel size'])
        self.voltage = float(self.general_options['voltage (kV)'])
        self.dose_per_frame = float(self.general_options['dose per frame (e/A^2)'])
        self.cs = float(self.general_options['cs'])
        self.ac = float(self.general_options['amplitude contrast'])

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
