import argparse
import shutil
from time import sleep
import os

parser = argparse.ArgumentParser(description='Move one file every x seconds')
parser.add_argument('directory', help='Target directory')
parser.add_argument('--t', type=int, default=30, help='Time delay (default 30s)')
parser.add_argument('--files', nargs='+')
args = parser.parse_args()


def copyfile_slow(files, dest, t):
    while bool(files):
        file = files.pop()
        shutil.copy(file, dest)
        sleep(t)

assert os.path.isdir(args.directory), print('Target directory does not exist!')
copyfile_slow(args.files, args.directory, args.t)
