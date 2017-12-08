import os
from threading import Timer
import pandas as pd
import subprocess
import shutil

def crop_image(input: str, output_directory: str, height: int):
    """

    :param input: path to image file
    :param output_directory: path to output directory
    :param height: height of the output PNG image in pixels
    :return:
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
    return

class ProcessTable:

    def __init__(self, path, stop_event):
        self.path = path
        self.t = 20 # refresh time in seconds
        self.file = os.path.join(self.path, 'process_table.csv')
        # lists are thread safe
        self.series = []
        self.columns = set()
        self.stop_event = stop_event
        self.dump(self.t)
        self.static_folder = os.path.join(self.path, 'static')
        self.static_motioncor = os.path.join(self.static_folder, 'motioncor')
        self.static_gctf = os.path.join(self.static_folder, 'gctf')
        os.mkdir(self.static_folder)
        os.mkdir(self.static_motioncor)
        os.mkdir(self.static_gctf)

    def addMic(self, micrograph_name, results):
        # store processing results as a pandas.Series object
        self.series.append(pd.Series(results, name=micrograph_name))
        self.columns = self.columns | results.keys()
        if 'motioncor_aligned_DW' in results:
            img = os.path.join(self.path, results['motioncor_aligned_DW'])
            crop_image(img, self.static_motioncor, 800)
        if 'gctf_ctf_fit' in results:
            img = os.path.join(self.path, results['gctf_ctf_fit'])
            crop_image(img, self.static_gctf, 800)


    def dump(self, t):
        df = pd.DataFrame(columns=self.columns)
        for series in self.series:
            df = df.append(series)
        if not df.empty:
            df = df.sort_values(by='created_at')
            print("Writing results to process table.")
            df['Defocus'] = df[["Defocus_U", "Defocus_V"]].mean(axis=1)
            df[['Defocus', 'Defocus_U', 'Defocus_V']] = df[['Defocus', 'Defocus_U', 'Defocus_V']] / 1000
            df[['Phase_shift']] = df[['Phase_shift']] / 180
            df['delta_Defocus'] = df["Defocus_U"] - df["Defocus_V"]
            df.to_csv(self.file, index_label='micrograph')

        if self.stop_event.is_set():
            return
        self.thread = Timer(t, self.dump)
        self.thread.start()
