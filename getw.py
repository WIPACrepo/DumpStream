#!/usr/bin/python3
import os
import json
import urllib.parse
import sys
import datetime
import subprocess
import string
import copy

DEBUGPROCESS = False

REPLACESTRING = '+++'

def getoutputsimplecommand(cmd):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #proc = subprocess.call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        if DEBUGPROCESS:
            print("===")
            print(output)
            print("===")
            print(error)
            print("===")
        if len(error) != 0:
            print('ErrorA:::', cmd)
            print(output)
            print(error)
            return ""
        else:
            return output
    except subprocess.CalledProcessError:
        if DEBUGPROCESS:
            print('ErrorB::::', cmd, " Failed to spawn")
        return ""
    except Exception:
        if DEBUGPROCESS:
            print([cmd, " Unknown error", sys.exc_info()[0]])
        return ""

curlcommand = '/usr/bin/curl'
target = 'http://archivecontrol.wipac.wisc.edu:80/bundles/specified/'
basicget = [curlcommand, '-X', 'GET', '-H', '\"Content-Type:application/x-www-form-urlencoded\"']
basicget = [curlcommand, '-sS', '-X', 'GET', '-H', 'Content-Type:application/x-www-form-urlencoded']
#basicget = [curlcommand, '-X', 'GET']


def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('@', ' ')

def depercent(stringToGo):
    return stringToGo.replace('%', '%25')

def dedouble(stringToGo):
    return stringToGo.replace('\"', '\%22')

def singletodouble(stringTo):
    return stringTo.replace('\'', '\"')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', '\,').replace(' ', '@')


onequote = r'"'
print(onequote)
oneslash = '\\'
quotequote = oneslash + onequote
#mumble = r'useCount=1 AND idealName LIKE ' + oneslash + '\"%data/exp/IceCube/2018/unbiased/PFRaw/0227/f5f40fb0-5e%' + oneslash + '\"'
mumble = r'useCount=1 AND size>872095068081'
#mumble = r'idealName LIKE ' + oneslash + '\"%xxx%' + oneslash + '\"'
#mumble = r'idealName LIKE ' + quotequote + '%xxx%' + quotequote
print(mumble)
#twisted = mangle(dedouble(depercent(mumble)))
twisted = mangle(depercent(mumble))

newurl = target + onequote + twisted + onequote
newurl = target + twisted
newget = copy.deepcopy(basicget)
newget.append(newurl)
print(newget)
#newg = ''
#for word in newget:
#    newg = newg + ' ' + word
#print('newg=',newg)
answer = getoutputsimplecommand(newget)
#print('answer=',answer)
#clam=singletodouble(str(answer))
#print(clam)
clam=singletodouble(answer.decode("utf-8"))
my_json = json.loads(clam)
number = len(my_json)
for i in range(number):
    local = my_json[i]
    print(local['idealName'], local['size'])
