#!/usr/bin/python3
import os
import json
import urllib.parse
import sys
import datetime
import subprocess
import string

def getoutputsimplecommand(cmd, ignoreInError):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        #print("===")
        #print(output)
        #print("===")
        #print(error)
        #print("===")
        if error != "":
            if not ignoreInError:
                print('Error:::', cmd, output, error)
                return ""
            for p in ignoreInError:
                if p in error:
                    return output	# Expected, ignore error message
            print('Error:::', cmd, output, error)
            return ""
        else:
            return output
    except subprocess.CalledProcessError:
        print('Error::::', cmd, " Failed to spawn")
        return ""
    except Exception:
        if not ignoreInError:
            print('Error::: unknown error', cmd, output, error)
            return ""
        for p in ignoreInError:
            if p in str(error):
                return output       # Expected, ignore error message
        print('Error:::', cmd, output, error)
        return ""
        print([cmd, " Unknown error", sys.exc_info()[0]])
        return ""

curlcommand = '/usr/bin/curl'
#target = 'http://archivecontrol.wipac.wisc.edu:5000/debug/'
target = 'http://archivecontrol.wipac.wisc.edu:5000/addbundle/'
ignoreErrorIfTheseShowUp = ['Receive', 'Speed']
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



with open('xxx.json') as json_file:
    data = json.load(json_file)
    #for p in data:
    #    print(p,data[p])
    mumble = unslash((str(data)))
    teststring = urllib.parse.quote_plus(mumble)
    sendstring = target + teststring
    comstr = [curlcommand, '-X', 'POST', sendstring]
    answer = getoutputsimplecommand(comstr, ignoreErrorIfTheseShowUp)
    print(answer)
