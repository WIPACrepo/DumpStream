import sys
import site
import os
import json
import urllib.parse
#import datetime
import subprocess
import socket
#import string
import copy

#####
# Define some constants
REPLACESTRING = '+++'
NERSCSTATI = ['Run', 'Halt', 'DrainNERSC', 'Error']
LOCALSTATI = ['Run', 'Halt', 'Drain', 'Error']
DEBUGPROCESS = False
# WARN if free scratch space is low
FREECUTLOCAL = 50000000
# How many slurm jobs can go at once?
SLURMCUT = 14
DEBUGLOCAL = True
if DEBUGLOCAL:
    sbatch = '/home/jbellinger/archivecontrol/nersctools/sbatch'
    rm = '/home/jbellinger/archivecontrol/nersctools/rm'
    hpss_avail = '/home/jbellinger/archivecontrol/nersctools/hpss_avail'
    df = '/home/jbellinger/archivecontrol/nersctools/df'
    mv = '/home/jbellinger/archivecontrol/nersctools/mv'
    squeue = '/home/jbellinger/archivecontrol/nersctools/squeue'
    myquota = '/home/jbellinger/archivecontrol/nersctools/myquota'
    logdir = '/home/jbellinger/archivecontrol/nersctools/SLURMLOGS'
    logdirold = '/home/jbellinger/archivecontrol/nersctools/SLURMLOGS/OLD'
    SCRATCHROOT = '/home/jbellinger/archivecontrol/nersctools/scratch'
    HSIROOT = '/home/jbellinger/archivecontrol/nersctools/hsi'
    hsibase = []
    BUNDLETREE = '/home/jbellinger/archivecontrol/jadetools/BUNDLE'
else:
    sbatch = '/usr/bin/sbatch'
    rm = '/usr/bin/rm'
    hpss_avail = '/usr/common/mss/bin/hpss_avail'
    df = '/usr/bin/df'
    mv = '/usr/bin/mv'
    squeue = '/usr/bin/squeue'
    myquota = '/usr/bin/myquota'
    logdir = '/global/homes/i/icecubed/SLURMLOGS'
    logdirold = '/global/homes/i/icecubed/SLURMLOGS/OLD'
    SCRATCHROOT = '/global/cscratch1/sd/icecubed/jade-disk'
    HSIROOT = '/home/projects/icecube'
    hsibase = ['/usr/common/mss/bin/hsi', '-q']
    BUNDLETREE = '/mnt/lfss/jade-lta/bundles-scratch/bundles?'

curlcommand = '/usr/bin/curl'
curltargethost = 'http://archivecontrol.wipac.wisc.edu:80/'

# I will have to figure out how to pack these in a command,
#  but for the moment here they are.  I drop 2 defaults,
#  the Partition and the #Nodes, since I don't care about
#  those.  I don't need the %.8u option either,
#  since I already know whose jobs they are.
# I can't be sure that mine will be the only icecubed jobs,
#  so I retain the %.8j for the job name
# The -h option means no headers printed, which should
#  simplify parsing.
SQUEUEOPTIONS = '-h -o \"%.18i %.8j %.2t %.10M %.42k %R\"'
# This will be associated with a sbatch option which I will
#  use as sbatchoption = SBATCHOPTIONS.format(filename, logdir, filename)
SBATCHOPTIONS = '--comment=\"{}\" --output={}/slurm_%j_{}.log'

targetfindbundles = curltargethost + 'bundles/specified/'
targetnerscinfo = curltargethost + 'nersccontrol/info/'
targetdumpinfo = curltargethost + 'dumpcontrol/info'
targetbundleinfo = curltargethost + 'bundles/specified/'
targettokeninfo = curltargethost + 'nersctokeninfo'
targetheartbeatinfo = curltargethost + 'heartbeatinfo/'
targetupdatebundle = curltargethost + 'updatebundle/'
targetaddbundle = curltargethost + 'addbundle/'
targetsetdumpstatus = curltargethost + '/dumpcontrol/update/status/'
targetsetdumppoolsize = curltargethost + '/dumpcontrol/update/poolsize/'
targetsetdumperror = curltargethost + '/dumpcontrol/update/bundleerror/'

basicgeturl = [curlcommand, '-sS', '-X', 'GET', '-H', 'Content-Type:application/x-www-form-urlencoded']
basicposturl = [curlcommand, '-sS', '-X', 'POST', '-H', 'Content-Type:application/x-www-form-urlencoded']

scales = {'B':0, 'KiB':0, 'MiB':.001, 'GiB':1., 'TiB':1000.}
snames = ['KiB', 'MiB', 'GiB', 'TiB']

GLOBUS_PROBLEM_SPACE = '/mnt/data/jade/problem_files/globus-mirror'
GLOBUS_DONE_SPACE = '/mnt/data/jade/mirror-cache'
GLOBUS_RUN_SPACE = '/mnt/data/jade/mirror-queue'
GLOBUS_OLD_SPACE = '/mnt/data/jade/mirror-old'
GLOBUS_INFLIGHT_LIMIT = 3

BundleStatusOptions = ['Untouched', 'JsonMade', 'PushProblem', 'PushDone', 'NERSCRunning', 'NERSCDone', \
        'NERSCProblem', 'NERSCClean', 'LocalDeleted', 'Abort', 'Retry']

# String manipulation stuff
def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/')

def unmangls(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace(r'\,', ',').replace('@', ' ')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', r'\,').replace(' ', '@')

def tojsonquotes(strFromPost):
    # Turn single into double quotes
    return strFromPost.replace("\'", "\"")

def fromjsonquotes(strFromPost):
    # Turn double into single quotes.  Won't use it much
    # here, but the remote jobs that feed this will
    return strFromPost.replace("\"", "\'")

def singletodouble(stringTo):
    return stringTo.replace('\'', '\"')

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

# timeout is in seconds
def getoutputsimplecommandtimeout(cmd, Timeout):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #proc = subprocess.call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate(Timeout)
        if len(error) != 0:
            print('ErrorA:::', cmd)
            return ""
        else:
            return output
    except subprocess.CalledProcessError:
        if DEBUGPROCESS:
            print('ErrorB::::', cmd, " Failed to spawn")
        return ""
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    except Exception:
        if DEBUGPROCESS:
            print([cmd, " Unknown error", sys.exc_info()[0]])
        return ""


def getoutputerrorsimplecommand(cmd, Timeout):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate(Timeout)
        returncode = proc.returncode
        return output, error, returncode
    except subprocess.CalledProcessError:
        print(cmd, " Failed to spawn")
        return "", "", 1
    except subprocess.TimeoutExpired:
        return 'TIMEOUT', 'TIMEOUT', 1
    except Exception:
        print(cmd, " Unknown error", sys.exc_info()[0])
        return "", 1


####
# Test parse byte-stream of list of dicts back into a list of strings
# each of which can be unpacked later into a dict
def stringtodict(instring):
    if len(instring) <= 1:
        return []
    countflag = 0
    initial = -1
    final = -1
    basic = []
    for num, character in enumerate(instring):
        if character == '{':
            countflag = countflag + 1
            if countflag == 1:
                initial = num
        if character == '}':
            countflag = countflag - 1
            if countflag == 0:
                basic.append(instring[initial:num+1])
    return basic
        


########################################################
# Main
#GLOBUS_PROBLEM_SPACE = '/mnt/data/jade/problem_files/globus-mirror'
#GLOBUS_DONE_SPACE = '/mnt/data/jade/mirror-cache'
#GLOBUS_RUN_SPACE = '/mnt/data/jade/mirror-queue'
#GLOBUS_OLD_SPACE = '/mnt/data/jade/mirror-old'

# Phase 0
# Look for space usage
# Log the space usage and date
# Done w/ phase 0
def Phase0():
    #command = ['/bin/df', '/mnt/lfss']
    command = ['/bin/df', '-BG', '/var/log']
    ErrorString = ''
    outp, erro, code = getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        ErrorString = ErrorString + ' Failed to df '
    else:
        lines = outp.splitlines()
        size = -1
        for line in lines:
            if '/var/log' in str(line):
                words = line.split()
                try:
                    sizeword = words[len(words)-3]
                    size = int(sizeword[0:-1])  # remove trailing G
                except:
                    ErrorString = ErrorString + ' Failed to df '
    #print(size)
    if len(ErrorString) > 0:
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle(ErrorString))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        return
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetsetdumppoolsize + str(size))
    answer = getoutputsimplecommandtimeout(posturl, 1)
    if answer != 'OK':
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle('Failed to set poolsize'))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        return
    return

#targetsetdumperror = curltargethost + '/dumpcontrol/update/bundleerror/'
#targetsetdumpstatus = curltargethost + '/dumpcontrol/update/status/'
#targetsetdumppoolsize = curltargethost + '/dumpcontrol/update/poolsize/'
GLOBUS_PROBLEM_SPACE = '/opt/testing/rest/test'
# 
# Phase 1	Look for problem files
# ls /mnt/data/jade/problem_files/globus-mirror
# If GLOBUS_PROBLEM_SPACE
# If alert file is only file present, delete it and we're done
# foreach .json file in the list
#    Update BundleStatus for each with 'PushProblem'
# Update CandC with status='Error', bundleError='problem_files'
# Done with phase 1
def Phase1():
    command = ['/bin/ls', GLOBUS_PROBLEM_SPACE]
    ErrorString = ''
    outp, erro, code = getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        ErrorString = ErrorString + ' Failed to ls ' + GLOBUS_PROBLEM_SPACE + ' '
    else:
        lines = outp.splitlines()
        print(len(lines))


# Phase 2	Look for transferred files
# ls GLOBUS_DONE_SPACE
# If no file present, done w/ phase 2
# foreach .json file in the list
#    Update BundleStatus for each with status = 'PushDone'
#    move .json to GLOBUS_OLD_SPACE
# Done with phase 2
#
# Phase 3	Look for new local files
# Get list of local bundle tree locations relevant to NERSC transfers
# Foreach tree
#   Find bundle files in each tree
#   Accumulate information in locallist
# If locallist is not empty
#   Build a query for contents of locallist
# Query BundleStatus for these
# Parse the return:
# If the file is not in BundleStatus, add it as 'Untouched'
# Done with phase 3
#
# Phase 4	Submit new files
# Check CandC for go/nogo
# If Error or Halt or Drain, done w/ phase 4
# Query BundleStatus for the bundlelist of 'Untouched' bundles,
# starting with the oldest
# If == 0, done w/ phase 4
# Query BundleStatus for the count of JsonMade bundles
# If > GLOBUS_INFLIGHT_LIMIT, done w/ phase 4
# foreach bundle in the bundlelist
#   if the running count > GLOBUS_INFLIGHT_LIMIT, done w/ phase 4
#   Create a .json file for this bundle in GLOBUS_RUN_SPACE
#   update the BundleStatus for this bundle to 'JSONMade' 
Phase0()
