# mpiapp

The MPIApp is a software tool for electron microscopy for pipeline processing of recorded micrographs.

## Setup
Download the repository.
Create a virtual environment for python and install requirements
```
conda create --name mpiapp
source activate mpiapp
pip install pyinotify mrcfile
conda install -c dsdale24 pyqt5
```
Launch the application with `python mpiapp.py`
