#!/usr/bin/python3
import os
import json
import urllib.parse
import sys
import datetime
import subprocess
import string

def getoutputsimplecommand(cmd):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        print("===")
        print(output)
        print("===")
        print(error)
        print("===")
        if error != "":
            print('Error:::', cmd, output, error)
            return ""
        else:
            return output
    except subprocess.CalledProcessError:
        print('Error::::', cmd, " Failed to spawn")
        return ""
    except Exception:
        print([cmd, " Unknown error", sys.exc_info()[0]])
        return ""

curlcommand = '/usr/bin/curl'
target = 'http://archivecontrol.wipac.wisc.edu:5000/debug/'

REPLACESTRING = '+++'
def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('@', ' ')


def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/')


teststring = 'nothing'
sendstring = target + teststring
comstr = ['/usr/bin/strace', '-o', '/scratch/jbellinger/post', curlcommand, '-X', 'POST', sendstring]
answer = getoutputsimplecommand(comstr)
print(answer)

