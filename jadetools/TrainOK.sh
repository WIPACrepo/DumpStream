#!/bin/sh
# TrainOK.sh
# Tell us whether the dinosaur data transfer system to NERSC is
#  stalled or stopped up somewhere.
# No arguments
# Thresholds are hardwired in the python script.  This isn't 
# ideal, but I don't want to spend that time on a temporary
# system.
cd /home/jade/toNERSC
source venv/bin/activate
python Nagios.py
exit $?
