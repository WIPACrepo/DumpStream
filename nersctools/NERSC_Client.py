import sys
import site
import os
import json
import urllib.parse
import datetime
import subprocess
import socket
import string
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
# but for the moment here they are.  I drop 2 defaults,
# the Partition and the #Nodes, since I don't care about
# those.  I don't need the %.8u option either,
# since I already know whose jobs they are.
# I can't be sure that mine will be the only icecubed jobs,
# so I retain the %.8j for the job name
SQUEUEOPTIONS = '-o \"%.18i %.8j %.2t %.10M %.42k %R\"'
# This will be associated with a sbatch option which I will
# use as sbatchoption = SBATCHOPTIONS.format(filename)
SBATCHOPTIONS = '--comment=\"{}\"'

targetfindbundles = curltargethost + 'bundles/specified/'
targettaketoken = curltargethost + 'nersctokentake/'
targetreleasetoken = curltargethost + 'nersctokenrelease/'
targetupdateerror = curltargethost + 'nersccontrol/update/nerscerror/'
targetupdatebundle = curltargethost + 'updatebundle/'

basicgeturl = [curlcommand, '-sS', '-X', 'GET', '-H', 'Content-Type:application/x-www-form-urlencoded']
basicposturl = [curlcommand, '-sS', '-X', 'POST', '-H', 'Content-Type:application/x-www-form-urlencoded']

scales = {'B':0, 'KiB':0, 'MiB':.001, 'GiB':1., 'TiB':1000.}
snames = ['KiB', 'MiB', 'GiB', 'TiB']


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
    except TimeoutExpired:
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
    except TimeoutExpired:
        return 'TIMEOUT', 'TIMEOUT', 1
    except Exception:
        print(cmd, " Unknown error", sys.exc_info()[0])
        return "", error, 1



def myhostname():
    return socket.gethostname().split('.')[0]


###
def abandon():
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetreleasetoken)
    # Ask for a 30-second timeout, in case of network issues
    answer = getoutputsimplecommandtimeout(posturl, 30)
    # If this fails, I can't update anything anyway
    if answer != 'OK':
        print(str(answer))
    sys.exit(1)


# I will do this enough to demand a utility for it
def flagBundleError(key):
    posturl = copy.deepcopy(basicposturl)
    comstring = mangle('UPDATE BundleStatus SET status=\'NERSCProblem\' WHERE bundle_status_id={}'.format(key))
    posturl.append(targetupdatebundle + comstring)
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetupdateerror + mangle('Error'))
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    return 

########################################################
#
# Separate the operation into different phases

# Get the token to run, if possible
def Phase0():
    # Negotiate for the token
    command = targettaketoken + myhostname()
    posturl = copy.deepcopy(basicposturl)
    posturl.append(command)
    # Ask for a 30-second timeout, in case of network issues
    answer = getoutputsimplecommandtimeout(posturl, 30)
    if answer != 'OK':
        sys.exit(0)

# Check for errors
def Phase1():
    NERSCErrorString = ''
    AbortFlag = False
    # First check hpss
    command = [hpss_avail, 'hpss']
    outp, erro, code = getoutputerrorsimplecommand(command, 5)
    if int(code) != 0:
        AbortFlag = True
        NERSCErrorString = NERSCErrorString + 'HPSS Not available '
    if outp == 'TIMEOUT':
        AbortFlag = True
        NERSCErrorString = NERSCErrorString + 'HPSS Not available '
    # Now, does scratch exist?
    command = [df, '/global/cscratch1']
    outp, erro, code = getoutputerrorsimplecommand(command, 2)
    if int(code) != 0:
        AbortFlag = True
        NERSCErrorString = NERSCErrorString + 'Scratch Not available '
    if outp == 'TIMEOUT':
        abandon()
        NERSCErrorString = NERSCErrorString + 'Scratch Not available '
    # Now check slurm
    command = [squeue, '-u', 'icecubed', '-M', 'escori']
    outp, erro, code = getoutputerrorsimplecommand(command, 15)
    if int(code) != 0:
        AbortFlag = True
        NERSCErrorString = NERSCErrorString + 'SLURM Not Working '
    allstuff = str(outp)
    if 'Error' in allstuff or 'error' in allstuff:
        AbortFlag = True
        NERSCErrorString = NERSCErrorString + 'SLURM Not Working '
    # Now check the quota
    command = [myquota]
    outp, erro, code = getoutputerrorsimplecommand(command, 15)
    cases = outp.splitlines()
    for p in cases:
        q = str(p)
        if q.find('cscratch1') > 0:
            words = q.split()
            value1 = 0.
            value2 = 0.
            for sn in snames:
                if sn in words[1]:
                    value1 = scales[sn]*float(words[1].split(sn)[0])
                if sn in words[2]:
                    value2 = scales[sn]*float(words[2].split(sn)[0])
    freespace = int(value2-value1)
    if freespace < FREECUT:
        NERSCErrorString = NERSCErrorString + 'Low Scratch Space '
        # This does not require us to quit, since we're draining the
        # scratch
    nerscerror = mangle(NERSCErrorString)
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetupdateerror + nerscerror)
    answer = getoutputsimplecommandtimeout(posturl, 30)
    if answer != 'OK':
        AbortFlag = True
    #
    if AbortFlag:
        abandon()


##
# Look for "copy into HPSS" jobs.  Are any done?
# Note that I moved the hsi -q ls -l into the slurm job,
# so all I should need to do is read the log file.

def Phase2():
    # Check NERSCandC status
    #  Quit if Halt or Error
    # Look for outstanding jobs: status= NERSCRunning, count=NC
    #  slurm query for running jobs
    #   if count of running jobs is equal to NC, return
    #  [[[[ CAN I MATCH THE slurm job ID TO THE FILE?
    #       I CAN include the file name in a comment! ]]]
    return

# Look for files globus-copied into NERSC (PushDone)

def Phase3():
    #
    # Look for files with PushDone as the status, pull all info
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetfindbundles + mangle('status = \"PushDone\"'))
    listofbundles = getoutputsimplecommandtimeout(geturl, 30)
    if listofbundles == "":
        return		# Nothing to do
    #
    # count the slurm jobs active and log files open
    # if count >= SLURMCUT
    #     DONE with this phase
    NERSCErrorString = ''
    command = [squeue, '-u', 'icecubed', '-M', 'escori']
    outp, erro, code = getoutputerrorsimplecommand(command, 15)
    if int(code) != 0:
        NERSCErrorString = NERSCErrorString + 'SLURM Not Working '
    allstuff = str(outp)
    if 'Error' in allstuff or 'error' in allstuff:
        NERSCErrorString = NERSCErrorString + 'SLURM Not Working '
    if NERSCErrorString != '':
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetupdateerror + mangle(NERSCErrorString))
        answer = getoutputsimplecommandtimeout(posturl, 30)
        return   # This is the last phase, so the abandon will happen
        # automatically
    lines = allstuff.splitlines()
    activeSlurm = len(lines) - 2
    if activeSlurm >= SLURMCUT:
        return		# #copy jobs is bottleneck.  Wait till something ends
    #
    # Check the log files, just in case ?
    # command = [logdir + 'summary']
    # outp, erro, code = getoutputsimplecommandtimeout(command, 2)
    # try:
    #     logcount = int(str(outp).split()[0])
    #     donecount = int(str(outp).split()[1])
    # except:
    #     print("Failed to get log info")
    # if logcount - donecount > activeSlurm:
    #     print("unfinished jobs > active jobs", str(logcount), str(donecount))
    #
    ####
    ## foreach file
    ##    create scratch file name
    ##    look for scratch file size
    ##    if it doesn't exist, or is the wrong size
    ##       log a NERSCProblem for the file and an Error in NERSCandC and abandon
    ##       if count < SLURMCOUNT
    ##           setup slurm job from templates
    ##           increment count
    ##           invoke sbatch
    ##           update bundle info with new status
    ##           if count >= SLURMCOUNT
    ##               DONE with this phase
    my_json = json.loads(singletodouble(listofbundles.decode("utf-8"))
    for bundle in my_json:
        idealname = bundle['idealName']
        size = int(bundle['size'])
        key = bundle['bundleStatus_id']
        scratchname = SCRATCHROOT + idealName
        command = ['ls', '-go', scratchname]
        outp, erro, code = getoutputerrorsimplecommand(command, 5)
        badFlag = False
        if int(code) != 0:
            badFlag = True
        else:
            ssize = (str(outp).split()[2])
            if ssize != size:
                badFlag = True
        if badFlag:
            flagBundleError(key)
            return	# Last phase, so no need to abandon()
        command = [sbatch, '/global/homes/i/icecubed/SLURMLOGS/xfer_put.sh', scratchname,
                '-o', '/global/homes/i/icecubed/SLURMLOGS/slurm-%j.out']        
        outp, erro, code = getoutputerrorsimplecommand(command, 5)
        if int(code) != 0:
            badFlag = True
        # I may check the code in more detail later
        if badFlag:
            flagBundleError(key)
            return	# Last phase, so no need to abandon()
    #
    return

############
# MAIN PROGRAM

Phase0()
Phase1()
Phase2()
Phase3()
abandon()
