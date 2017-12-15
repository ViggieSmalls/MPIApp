import os
from threading import Timer
import pandas as pd
import subprocess
import shutil
import logging

def crop_image(input: str, output_directory: str, height: int):
    """
    Calls imod programs to convert an mrc file to png for displaying it in the web page
    :param input: path to image file
    :param output_directory: path to output directory
    :param height: height of the output PNG image in pixels
    """
    for executable in ["header", "newstack", "mrc2tif"]:
        assert shutil.which(executable), "Executable {} does not exist!".format(executable)

    # get image size
    get_size = subprocess.Popen(["header", "-size", input], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = get_size.communicate()
    x,y,z = map(int, out.split())
    # scale image
    tmp = os.path.join("/tmp", os.path.basename(input))
    scale_factor = float(y) / height
    scale_image = subprocess.Popen(["newstack", "-shrink", str(scale_factor), input, tmp],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    scale_image.wait()
    # convert to png
    png = os.path.splitext(os.path.basename(input))[0] + ".png"
    png_path = os.path.join(output_directory, png)
    subprocess.Popen(["mrc2tif", "-p", tmp, png_path],
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE).wait()
    os.remove(tmp)
    return

class ProcessTable:

    def __init__(self, path, stop_event, timeout=20):
        self.logger = logging.getLogger('mpi_application')
        self.logger.info('Initializing the process table in the output directory {}'.format(path))
        self.path = path
        self.logger.info('Process table contents will be refreshed every {} seconds'.format(timeout))
        self.t = timeout
        self.stop_event = stop_event
        self.start()

    def start(self):
        """
        Initializes the process table. If everything is successful, the function dump is called
        which starts writing every x seconds to the process table file
        """
        self.file = os.path.join(self.path, 'process_table.csv')
        self.gctf_star = os.path.join(self.path, 'micrographs_all_gctf.star')
        shutil.copy('templates/project.html', self.path)

        try:
            open(self.file, 'a').close()
            self.logger.info('Process table file was created at {}'.format(self.file))
        except:
            self.logger.error('Could not create process table output file at location {}'.format(self.file))
            return

        self.logger.info('Creating static folders inside the output directory')
        self.static_folder = os.path.join(self.path, 'static')
        self.static_js = os.path.join(self.static_folder, 'js')
        self.static_motioncor = os.path.join(self.static_folder, 'motioncor')
        self.static_gctf = os.path.join(self.static_folder, 'gctf')
        if not os.path.isdir(self.static_folder):
            os.mkdir(self.static_folder)
            self.logger.debug('Created folder {}'.format(self.static_folder))
        if not os.path.isdir(self.static_js):
            os.mkdir(self.static_js)
            self.logger.debug('Created folder {}'.format(self.static_js))
            shutil.copy('static/js/histogram.js', self.static_js)
        if not os.path.isdir(self.static_motioncor):
            os.mkdir(self.static_motioncor)
            self.logger.debug('Created folder {}'.format(self.static_motioncor))
        if not os.path.isdir(self.static_gctf):
            os.mkdir(self.static_gctf)
            self.logger.debug('Created folder {}'.format(self.static_gctf))
        self.logger.info('Finished creating static folders')

        try:
            self.series = []
            self.columns = set()
            self.dump()
        except:
            self.logger.error('Could not write to {}'.format(self.file))
            self.logger.error('Process table could not be initialized')
            return

    def addMic(self, micrograph_name, results: dict):
        """
        Transforms the results to a pandas.Series object with the name of the micrograph and
        adds it to an internal list (thread safe).
        :param micrograph_name: base name of the micrograph
        :param results: A dictionary containing processing results of the micrograph
        """
        self.logger.info('Adding micrograph {} to ProcessTable'.format(micrograph_name))
        self.logger.debug('Results are stored as a pandas.Series object')
        self.series.append(pd.Series(results, name=micrograph_name))
        self.columns = self.columns | results.keys()
        #TODO create static images as part of the micrograph processing
        if 'motioncor_aligned_DW' in results:
            img = os.path.join(self.path, results['motioncor_aligned_DW'])
            crop_image(img, self.static_motioncor, 800)
        if 'gctf_ctf_fit' in results:
            img = os.path.join(self.path, results['gctf_ctf_fit'])
            crop_image(img, self.static_gctf, 800)

    def dump(self):
        """
        Creates a DataFrame from all stored Series objects containing
        the micrograph names and their processing results.
        Performs some pre processing of the columns and writes the contents of
        the DataFrame to the process_table file
        """
        self.logger.debug('Converting {} entries to pandas.DataFrame object'.format(len(self.columns)))
        df = pd.DataFrame(columns=self.columns)
        for series in self.series:
            df = df.append(series)
        if not df.empty:
            #create process_table.csv
            df = df.sort_values(by='created_at')
            df['Defocus'] = df[["Defocus_U", "Defocus_V"]].mean(axis=1)
            df[['Defocus', 'Defocus_U', 'Defocus_V']] = df[['Defocus', 'Defocus_U', 'Defocus_V']] / 1000
            df[['Phase_shift']] = df[['Phase_shift']] / 180
            df['delta_Defocus'] = df["Defocus_U"] - df["Defocus_V"]

        self.logger.debug('Writing to process table')
        df.to_csv(self.file, index_label='micrograph')
        self.logger.debug('Finished writing contents of DataFrame to {}'.format(os.path.basename(self.file)))

        if not df.empty:
            # write gctf star file
            self.logger.debug('Writing gctf star file')
            _rln = df.filter(regex=("^_rln.*"))
            keys = list(_rln.columns)
            d = [i.split() for i in keys]
            d = [(i[0], int(i[1][1:])) for i in d]
            sorted_list = sorted(d, key=lambda x: x[1])
            columns = [i[0] + ' #' + str(i[1]) for i in sorted_list]
            rln = _rln[columns]
            rln['_rlnMicrographName #1'] = df['motioncor_aligned_no_DW']
            rln['_rlnCtfImage #2'] = df['gctf_ctf_fit'].apply(lambda s: s+':mrc')
            rln.to_csv(self.gctf_star, index=False, header=False, sep='\t')

            with open(self.gctf_star, 'r+') as f:
                content = f.read()
                f.seek(0, 0)
                f.write('data_\nloop_\n' + '\n'.join(columns) + '\n' + content)
            self.logger.debug('Finished writing gctf star file')


        if self.stop_event.is_set():
            self.logger.info('Stop event is set. Closing process table')
            return
        else:
            self.thread = Timer(self.t, self.dump)
            self.thread.start()
