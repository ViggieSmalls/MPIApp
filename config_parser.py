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

        self.motioncor_parameters = ['InMrc', 'InTiff', 'InSuffix', 'OutMrc', 'ArcDir', 'Gain', 'TmpFile', 'LogFile', 'Serial', 'MaskCent', 'MaskSize', 'Patch', 'Iter', 'Tol', 'Bft', 'StackZ', 'FtBin', 'InitDose', 'FmDose', 'PixSize', 'kV', 'Throw', 'Trunc', 'Group', 'FmRef', 'OutStack', 'RotGain', 'FlipGain', 'Align', 'Tilt', 'Mag', 'Crop', 'Gpu']
        self.gctf_parameters = ['apix', 'kV', 'cs', 'ac', 'phase_shift_L', 'phase_shift_H', 'phase_shift_S', 'phase_shift_T', 'dstep', 'defL', 'defH', 'defS', 'astm', 'bfac', 'resL', 'resH', 'boxsize', 'do_EPA', 'EPA_oversmp', 'overlap', 'convsize', 'do_Hres_ref', 'Href_resL', 'Href_resH', 'Href_bfac', 'B_resL', 'B_resH', 'do_mdef_refine', 'mdef_aveN', 'mdef_fit', 'mdef_ave_type', 'do_local_refine', 'local_radius', 'local_avetype', 'local_boxsize', 'local_overlap', 'local_resL', 'local_resH', 'refine_local_astm', 'refine_input_ctf', 'defU_init', 'defV_init', 'defA_init', 'B_init', 'defU_err', 'defV_err', 'defA_err', 'B_err', 'do_phase_flip', 'do_validation', 'ctfout_resL', 'ctfout_resH', 'ctfout_bfac', 'input_ctfstar', 'boxsuffix', 'ctfstar', 'logsuffix', 'write_local_ctf', 'plot_res_ring', 'do_unfinished', 'skip_check_mrc', 'skip_check_gpu', 'gid']

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
