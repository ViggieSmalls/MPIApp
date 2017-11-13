# mpiapp

The MPIApp is a software tool written in Python, that allows automation of repetitive tasks by using several external Python libraries. The software include following Python libraries:
* Pyinotify: Detection of new file system changes, e.g. incoming files
* Celery: Distribution of processing tasks in the network
* Flask: Web server for visualisation
Additional libraries like doit can be used to simplify the scripting of complex tasks with limited knowledge about Python.

## Setup
Create a virtual environment for python and install requirements
```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```
You might need to install the python3-tk package to use the gui interface:
```
sudo apt-get install python3-tk
```
