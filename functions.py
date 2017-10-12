import pandas as pd
import numpy as np
from threading import Timer
from pandas import Series
from configobj import ConfigObj
import subprocess
import os, shutil


def gctf(micrograph: str, config: dict, output_dir: str, static_dir: str, gpu_id: int, ccc: float = 0.75) -> Series:
    """

    :param micrograph: micrograph file
    :param config: dictionary with parsed configuration options
    :param gpu_id: ID of the GPU on which the process will run
    :param output_dir: path to output directory
    :param static_dir: path to static files directory
    :param ccc: cutoff cross correlation coefficient
    :return: pd.Series with name=micrograph_id and index=['Defocus_U', 'Defocus_V', 'Angle', 'Phase_shift', 'Resolution']
    """
    micrograph_id, micrograph_ext = os.path.splitext(micrograph)
    staticfiles_dir = os.path.join(static_dir, 'gctf')
    gctf_output_dir = os.path.join(output_dir, 'gctf')
    if not os.path.isdir(gctf_output_dir): os.mkdir(gctf_output_dir)
    if not os.path.isdir(staticfiles_dir): os.mkdir(staticfiles_dir)
    executable = config['gctf']['executable']
    cmd = [executable]
    options = config['gctf']['options']
    options['gid'] = str(gpu_id)

    for k, v in options.items():
        cmd.extend(['--' + k, v])

    # FIXME: or .mrcs
    input_micrograph = os.path.join(output_dir, 'motioncor', micrograph_id + '.mrc')
    target = os.path.join(gctf_output_dir, os.path.basename(input_micrograph))
    try: os.symlink(input_micrograph, target)
    except FileExistsError:
        os.unlink(target)
        os.symlink(input_micrograph, target)

    cmd.append(target)

    cmd = ' '.join(cmd)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    process.wait()

    gctf_log = os.path.join(gctf_output_dir, micrograph_id + '_gctf.log')
    epa_log = os.path.join(gctf_output_dir, micrograph_id + '_EPA.log')
    out, err = process.communicate()
    log = out.decode('utf-8')

    with open(gctf_log, "w") as logfile:
        logfile.write(log)

    for line in reversed(log.split('\n')):
        if line.endswith('Final Values'):
            data = np.array(line.split()[:-2]).astype(float)
            gctf_series = Series(data, index=['Defocus_U', 'Defocus_V', 'Angle', 'Phase_shift', 'CCC'],
                                 name=micrograph_id)

            # FIXME: first row misses!
            epa_df = pd.read_csv(epa_log, sep='\s+',
                                 names=['Resolution', '|CTFsim|', 'EPA( Ln|F| )', 'EPA(Ln|F| - Bg)', 'CCC'],
                                 header=1)
            index = epa_df.CCC.lt(ccc).idxmax()
            res = epa_df.iloc[index]['Resolution']
            gctf_series['Resolution'] = res
            del gctf_series['CCC']

            shutil.copy(gctf_log, staticfiles_dir)
            shutil.copy(epa_log, staticfiles_dir)
            power_spectrum = os.path.join(gctf_output_dir, micrograph_id + '.ctf')
            gctf_series['vis_PowerSpectrum'] = crop_image(power_spectrum, staticfiles_dir, 800)
            gctf_series['vis_gctf-log'] = os.path.join(staticfiles_dir, os.path.basename(gctf_log))
            gctf_series['vis_EPA-log'] = os.path.join(staticfiles_dir, os.path.basename(epa_log))
            return gctf_series

    return Series()

def motioncor(micrograph: str, config: dict, output_dir: str, static_dir: str, gpu_id: int):
    micrograph_id, micrograph_ext = os.path.splitext(micrograph)
    staticfiles_dir = os.path.join(static_dir, 'motioncor')
    outputfiles_dir = os.path.join(output_dir, 'motioncor')
    if not os.path.isdir(outputfiles_dir): os.mkdir(outputfiles_dir)
    if not os.path.isdir(staticfiles_dir): os.mkdir(staticfiles_dir)

    executable = config['motioncor']['executable']
    cmd = [executable]
    options = config['motioncor']['options']

    if 'InTiff' in options:
        options['InTiff'] = micrograph_id + '.tif'
    elif 'InMrc' in options:
        options['InMrc'] = micrograph_id + '.mrc'
    if 'OutMrc' in options:
        options['OutMrc'] = os.path.join(outputfiles_dir, micrograph_id + '.mrc')
    elif 'OutStack' in options:
        options['OutStack'] = os.path.join(outputfiles_dir, micrograph_id + '.mrcs')
    options['Gpu'] = str(gpu_id)

    for k, v in options.items():
        cmd.extend(['-' + k, v])
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()

    logfile = os.path.join(outputfiles_dir, micrograph_id + '_DriftCorr.log')
    out, err = process.communicate()
    with open(logfile, "w") as log:
        log.write(out.decode('utf-8'))

    res_DW = os.path.join(outputfiles_dir, micrograph_id + '_DW.mrc')
    res_noDW = os.path.join(outputfiles_dir, micrograph_id + '.mrc')
    shutil.copy(logfile, staticfiles_dir)

    data = {
        'res_DW' : res_DW,
        'res_noDW' : res_noDW,
        'res_log' : logfile,
        'vis_DW' : crop_image(res_DW, staticfiles_dir, 800),
        'vis_noDW' : crop_image(res_noDW, staticfiles_dir, 800),
        'vis_log' : os.path.join(staticfiles_dir, os.path.basename(logfile)),
    }


    return Series(data, name=micrograph_id)

def my_parser(config_file: str) -> dict:
    configurations = ConfigObj(config_file)

    # motioncorr
    motioncor_executable = configurations['motioncor']['run']['executable']
    motioncor_options = configurations['motioncor']["options"]
    if configurations['motioncor']['run']['input file type'] == "tif":
        motioncor_options['InTiff'] = ""
    elif configurations['motioncor']['run']['input file type'] == "mrc":
        motioncor_options['InMrc'] = ""
    if configurations['motioncor']['run']['output file type'] == "mrc":
        motioncor_options['OutMrc'] = ""
    elif configurations['motioncor']['run']['output file type'] == "stack":
        motioncor_options["OutStack"] = ""
    motioncor_options['kV'] = configurations['voltage (kV)']
    motioncor_options['PixSize'] = configurations['pixel size']
    motioncor_options['Gpu'] = None
    try:
        bin = int(motioncor_options['FtBin'])
        configurations['pixel size'] = str(float(configurations['pixel size']) * bin)
    except KeyError:
        pass

    #gctf
    gctf_executable = configurations['gctf']['run']['executable']
    gctf_options = configurations['gctf']["options"]
    gctf_options['ac'] = configurations['amplitude contrast']
    gctf_options['cs'] = configurations['spherical aberration']
    gctf_options['kV'] = configurations['voltage (kV)']
    gctf_options['apix'] = configurations['pixel size']
    gctf_options['gid'] = None
    gctf_options['ctfstar'] = '/tmp/trash'

    return {
        'motioncor': {
            'executable': motioncor_executable,
            'options': motioncor_options
        },
        'gctf': {
            'executable': gctf_executable,
            'options': gctf_options
        }
    }


def crop_image(input: str, output_directory: str, height: int) -> str:
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
    return png_path

