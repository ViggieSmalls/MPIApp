import mrcfile
import numpy as np
from scipy import misc
from PIL import Image
import logging
from skimage import exposure

f = '20S_001_Mar28_14.59.32_DW.mrc'
logging.captureWarnings(True)

with mrcfile.mmap(f, mode='r+', permissive=True) as mrc:
    mrc.header.map = mrcfile.MAP_ID # output .mrc files from motioncor need this correction
    mrc.update_header_from_data()

mrc = mrcfile.open(f)
ary = np.squeeze(mrc.data) # remove single-dimensional entries
img_eq = exposure.equalize_hist(ary)
misc.imsave('image.png', img_eq)
image = Image.open('image.png')
image.thumbnail((400,400), Image.ANTIALIAS)
image.save('output.png')
