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
DEBUGPROCESS = False
# WARN if free scratch space is low
FREECUT = 500
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

basicgeturl = [curlcommand, '-sS', '-X', 'GET', '-H', 'Content-Type:application/x-www-form-urlencoded']
basicposturl = [curlcommand, '-sS', '-X', 'POST', '-H', 'Content-Type:application/x-www-form-urlencoded']

scales = {'B':0, 'KiB':0, 'MiB':.001, 'GiB':1., 'TiB':1000.}
snames = ['KiB', 'MiB', 'GiB', 'TiB']


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
        return "", error, 1


######
# Write out information
def logit(string1, string2):
    print(string1 + '  ' + string2)
    return

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


####
# NERSC status   
geturl = copy.deepcopy(basicgeturl)
geturl.append(targetnerscinfo)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    #print(outp)
    my_json = json.loads(singletodouble(outp.decode('utf-8')))
    nstats = (my_json['status'] + ' | ' + my_json['nerscError'] + ' | '
              + str(my_json['nerscSize']) + ' | ' + str(my_json['lastChangeTime']))
logit('NERSCStatus= ', nstats)


####
# NERSC token status
geturl = copy.deepcopy(basicgeturl)
geturl.append(targettokeninfo)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    my_json = json.loads(singletodouble(outp.decode('utf-8')))
    tname = 'NULL'
    if my_json['hostname'] != '':
        tname = my_json['hostname']
    nstats = tname + ' at ' + str(my_json['lastChangeTime'])
logit('NERSCToken= ', nstats)

####
# NERSC heartbeats
geturl = copy.deepcopy(basicgeturl)
geturl.append(targetheartbeatinfo)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    trialbunch = stringtodict(str(outp))
    #print(trialbunch)
    nstats = 'Beats: '
    for chunk in trialbunch:
        #print(chunk)
        my_json = json.loads(singletodouble(chunk))  #outp.decode('utf-8')))
        nstats = nstats + '| ' + my_json['hostname'] + '::' + str(my_json['lastChangeTime'])
logit('NERSCHeartbeats= ', nstats)


####
# local status   
geturl = copy.deepcopy(basicgeturl)
geturl.append(targetdumpinfo)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    #print(outp)
    my_json = json.loads(singletodouble(outp.decode('utf-8')))
    nstats = (my_json['status'] + ' | ' + my_json['bundleError'] + ' | '
              + str(my_json['bundlePoolSize']) + ' | ' + str(my_json['lastChangeTime']))
logit('LocalStatus= ', nstats)


####
# How many bundles have each status?
# I will probably get fancier later.  For now, just this.
nstats = ''
for opt in BundleStatusOptions:
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetupdatebundle + mangle('SELECT COUNT(*) FROM BundleStatus where status = \"' + opt + '\"'))
    outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
    #print(outp)
    if int(code) != 0:
        nstats = nstats + 'DB Failure'
    else:
        my_json = json.loads(singletodouble(outp.decode('utf-8')))
        nstats = nstats + ' | ' + opt + ':' + str(my_json['COUNT(*)'])
logit('BundleStatusCounts= ', nstats)
