import sys
import datetime
#import socket
#from getopt import getopt
import os
import subprocess
#from subprocess import Popen, PIPE
import xml.etree.ElementTree as ET
#import glob
import string


def getoutputsimplecommand(cmd):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        if error != "":
            print([cmd, error])
            return ""
        else:
            return output
    except subprocess.CalledProcessError:
        print([cmd, " Failed to spawn"])
        return ""
    except Exception:
        print([cmd, " Unknown error", sys.exc_info()[0]])
        return ""


#####
#
#teststring = '{"key1":"v1", "key2":"able+++baker"}'
teststring = '{"key1":"v1"\,"key2":"able+++baker"}'
target = "http://archivecontrol.wipac.wisc.edu:5000/dumpcontrol"
args = ["-X", "POST", "-H", "\"Content-Type: application/json\""]
command = "/usr/bin/curl"

comstr = [command]
for x in args:
    comstr.append(x)
comstr.append(target+teststring)

answer = getoutputsimplecommand(comstr)
print(answer)
print("========")
print(len(answer))
