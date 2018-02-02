my_options = {
    'kV': 300,
    'apix': 1.0,
    'cs': 2.62,
    'ac': 0.1,
    'Href_bfac': 50.0,
    'boxsize': 1024.0,
    'do_validation': 1.0,
    'phase_shift_L': 10.0,
    'defS': 500.0,
    'phase_shift_S': 10.0,
    'resH': 3.0,
    'defL': 3000.0,
    'B_resL': 20.0,
    'do_Hres_ref': 1,
    'estimate_B': 1,
    'do_EPA': 1,
    'phase_shift_H': 175.0,
    'phase_shift_T': 1.0,
    'B_resH': 3.0,
    'defH': 7000.0,
    'astm': 1000.0,
    'bfac': 100.0,
    'dstep': 1.06,
    'Href_resH': 3.0,
    'Href_resL': 15.0,
    'refine_after_EPA': 0,
    'resL': 20.0,
    'convsize': 30.0
}

import os

test = {
    'kV': float,
    'InMrc': os.path.isfile,
    'Gain': os.path.isfile,
    'PixSize': float,
    'FtBin': int,
    'Patch': list, # every string can be a list
}