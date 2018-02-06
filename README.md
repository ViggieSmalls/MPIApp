# mpiapp

The MPIApp is a software tool for electron microscopy for pipeline processing of recorded micrographs.

## Setup
Download the repository.
Create a virtual environment for python and install requirements
```
conda create --name mpiapp python=3.5 pyqt=5
source activate mpiapp
pip install pyinotify mrcfile
conda install pandas matplotlib scipy scikit-image
```
Launch the application with `python mpiapp.py`
