#!/usr/bin/python3
import sys
import site
python_home = '/opt/testing/rest/venv'

#......................

if '/opt/testing/rest/' not in list(sys.path):
    sys.path.insert(0,'/opt/testing/rest/')
if '/opt/testing/' not in list(sys.path):
    sys.path.insert(0,'/opt/testing/')
from rest import app as application
