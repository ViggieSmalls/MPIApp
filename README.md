# mpiapp

The MPIApp is a software tool written in Python, that allows automation of repetitive tasks.
* Pyinotify: Detection of new file system changes, e.g. incoming files

## Setup
Create a virtual environment for python and install requirements
```
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```
You might need to install the python3-tk package to use the gui interface:
```
sudo apt-get install python3-tk
```
