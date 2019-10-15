# NERSC_Client.py.base => NERSC_Client.py
import sys
# IMPORT_utils.py
# Assumes "import sys"
import site
import json
import urllib.parse
import subprocess
import copy
import os
import uuid
import socket

DEBUGIT = True

#####
# IMPORT CODE_utils.py
#####
# Define some constants
REPLACESTRING = '+++'
REPLACENOT = '==='
NERSCSTATI = ['Run', 'Halt', 'DrainNERSC', 'Error']
LOCALSTATI = ['Run', 'Halt', 'Drain', 'Error']
DEBUGPROCESS = False
# WARN if free scratch space is low
FREECUTLOCAL = 50000000
FREECUTNERSC = 500
# How many slurm jobs can go at once?
SLURMCUT = 14
DEBUGLOCAL = False
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
targettaketoken = curltargethost + 'nersctokentake/'
targetreleasetoken = curltargethost + 'nersctokenrelease/'
targetupdateerror = curltargethost + 'nersccontrol/update/nerscerror/'
targetnerscinfo = curltargethost + 'nersccontrol/info/'
targetdumpinfo = curltargethost + 'dumpcontrol/info'
targetbundleinfo = curltargethost + 'bundles/specified/'
targettokeninfo = curltargethost + 'nersctokeninfo'
targetheartbeatinfo = curltargethost + 'heartbeatinfo/'
targetupdatebundle = curltargethost + 'updatebundle/'
targetupdatebundleerr = curltargethost + 'updatebundleerr/'
targetnerscinfo = curltargethost + 'nersccontrol/info/'
targetaddbundle = curltargethost + 'addbundle/'
targetsetdumpstatus = curltargethost + '/dumpcontrol/update/status/'
targetsetdumppoolsize = curltargethost + '/dumpcontrol/update/poolsize/'
targetsetdumperror = curltargethost + '/dumpcontrol/update/bundleerror/'
targettree = curltargethost + '/tree/'
targetuntouchedall = curltargethost + '/bundles/alluntouched/'

basicgeturl = [curlcommand, '-sS', '-X', 'GET', '-H', 'Content-Type:application/x-www-form-urlencoded']
basicposturl = [curlcommand, '-sS', '-X', 'POST', '-H', 'Content-Type:application/x-www-form-urlencoded']

scales = {'B':0, 'KiB':0, 'MiB':.001, 'GiB':1., 'TiB':1000.}
snames = ['KiB', 'MiB', 'GiB', 'TiB']

GLOBUS_PROBLEM_SPACE = '/mnt/data/jade/problem_files/globus-mirror'
GLOBUS_DONE_SPACE = '/mnt/data/jade/mirror_cache'
GLOBUS_RUN_SPACE = '/mnt/data/jade/mirror_queue'
GLOBUS_DONE_HOLDING = '/mnt/data/jade/mirror_old'
GLOBUS_PROBLEM_HOLDING = '/mnt/data/jade/mirror_problem_files'
GLOBUS_INFLIGHT_LIMIT = 3

BundleStatusOptions = ['Untouched', 'JsonMade', 'PushProblem', 'PushDone', 'NERSCRunning', 'NERSCDone', \
        'NERSCProblem', 'NERSCClean', 'LocalDeleted', 'Abort', 'Retry']

# String manipulation stuff
def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING).replace('!', REPLACENOT)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/').replace(REPLACENOT, '!')

def unmangls(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace(r'\,', ',').replace('@', ' ').replace(REPLACENOT, '!')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', r'\,').replace(' ', '@').replace('!', REPLACENOT)

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
            print('ErrorA:::', cmd, '::::', error)
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
            print('ErrorA:::', cmd, '::-::', error)
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
        return "", "", 1

######
# Write out information.  Utility in case I want to do
# something other than simply print
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


def massage(answer):
    try:
        relaxed = str(answer.decode("utf-8"))
    except:
        try:
            relaxed = str(answer)
        except:
            relaxed = answer
    return relaxed


def globusjson(uuid, localdir, remotesystem, idealdir): 
    outputinfo = '{\n'
    outputinfo = outputinfo + '  \"component\": \"globus-mirror\",\n'
    outputinfo = outputinfo + '  \"version\": 1,\n'
    outputinfo = outputinfo + '  \"referenceUuid\": \"{}\",\n'.format(uuid)
    outputinfo = outputinfo + '  \"mirrorType\": \"bundle\",\n'
    outputinfo = outputinfo + '  \"srcLocation\": \"IceCube Gridftp Server\",\n'
    outputinfo = outputinfo + '  \"srcDir\": \"{}\",\n'.format(localdir)
    outputinfo = outputinfo + '  \"dstLocation\": \"{}\",\n'.format(remotesystem)
    outputinfo = outputinfo + '  \"dstDir\": \"{}\",\n'.format(idealdir)
    outputinfo = outputinfo + '  \"label\": \"Jade-LTA mirror lustre to {}\",\n'.format(remotesystem)
    outputinfo = outputinfo + '  \"notifyOnSucceeded\": false,\n'
    outputinfo = outputinfo + '  \"notifyOnFailed\": true,\n'
    outputinfo = outputinfo + '  \"notifyOnInactive\": true,\n'
    outputinfo = outputinfo + '  \"encryptData\": false,\n'
    outputinfo = outputinfo + '  \"syncLevel\": 1,\n'
    outputinfo = outputinfo + '  \"verifyChecksum\": false,\n'
    outputinfo = outputinfo + '  \"preserveTimestamp\": false,\n'
    outputinfo = outputinfo + '  \"deleteDestinationExtra\": false,\n'
    outputinfo = outputinfo + '  \"persistent\": true\n'
    outputinfo = outputinfo + '}'
    return outputinfo

def myhostname():
    return socket.gethostname().split('.')[0]


###
def abandon():
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetreleasetoken)
    # Ask for a 30-second timeout, in case of network issues
    answer = getoutputsimplecommandtimeout(posturl, 30)
    # If this fails, I can't update anything anyway
    if 'OK' not in str(answer):
        print('abandon fails with', str(answer))
    sys.exit(1)

def release():
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetreleasetoken)
    # Ask for a 30-second timeout, in case of network issues
    answer = getoutputsimplecommandtimeout(posturl, 30)
    # If this fails, I can't update anything anyway
    if 'OK' not in str(answer):
        print('release fails with', str(answer))
    sys.exit(1)



# I will do this enough to demand a utility for it
def flagBundleError(key):
    posturl = copy.deepcopy(basicposturl)
    comstring = mangle('UPDATE BundleStatus SET status=\'NERSCProblem\' WHERE bundleStatus_id={}'.format(key))
    posturl.append(targetupdatebundle + comstring)
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    # Set NERSC status to Error also
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetupdateerror + mangle('Error'))
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    return 
# Announce that it is running
def flagBundleRunning(key):
    posturl = copy.deepcopy(basicposturl)
    comstring = mangle('UPDATE BundleStatus SET status=\'NERSCRunning\' WHERE bundleStatus_id={}'.format(key))
    posturl.append(targetupdatebundle + comstring)
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    if 'OK' not in str(outp):
        print('Failure in updating BundleStatus to NERSCRunning for', str(key))
    return

# Announce that it is done
def flagBundleDone(key):
    posturl = copy.deepcopy(basicposturl)
    comstring = mangle('UPDATE BundleStatus SET status=\'NERSCDone\' WHERE bundleStatus_id={}'.format(key))
    posturl.append(targetupdatebundle + comstring)
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    if 'OK' not in str(outp):
        print('Failure in updating BundleStatus to NERSCDone for', str(key))
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
    if 'OK' not in str(answer):
        if DEBUGIT:
            print(answer)
            print('Failed to get token')
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
    #if DEBUGIT:
    #    print('myquota', outp)
    cases = outp.splitlines()
    #if DEBUGIT:
    #    print(cases[1])
    for p in cases:
        q = str(p)
        #if DEBUGIT:
        #    print('q',q)
        if q.find('cscratch1') > 0:
            words = q.split()
            value1 = 0.
            value2 = 0.
            for sn in snames:
                if sn in words[1]:
                    value1 = scales[sn]*float(words[1].split(sn)[0])
                if sn in words[2]:
                    value2 = scales[sn]*float(words[2].split(sn)[0])
    freespace = 0
    try:
        freespace = int(value2-value1)
    except:
        print('value1, value2', value1, value2)
    if freespace < FREECUTNERSC:
        NERSCErrorString = NERSCErrorString + 'Low Scratch Space '
        # This does not require us to quit, since we're draining the
        # scratch
    posturl = copy.deepcopy(basicposturl)
    if len(NERSCErrorString) > 0:
        posturl.append(targetupdateerror + mangle(NERSCErrorString))
    else:
        posturl.append(targetupdateerror + 'clear')
    answer = getoutputsimplecommandtimeout(posturl, 30)
    if 'OK' not in str(answer):
        print('Not OK somehow', str(answer))
        AbortFlag = True

    #
    if AbortFlag:
        if DEBUGIT:
            print('NERSCErrorString=', NERSCErrorString)
        abandon()


##
# Look for "copy into HPSS" jobs.  Are any done?
# Note that I moved the hsi -q ls -l into the slurm job,
# so all I should need to do is read the log file.

def Phase2():
    # Check NERSCandC status
    #  Quit if Halt or Error or fails to get info
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetnerscinfo)
    outp, erro, code = getoutputerrorsimplecommand(geturl, 5)
    if int(code) != 0:
        if DEBUGIT:
            print('Phase2-output=', str(outp))
            print(str(erro))
            print(str(code))
        return
        # Failed to get information.  It's a waste of time trying to
        # set an error when there are network problems
    my_json = json.loads(singletodouble(outp.decode("utf-8")))
    if my_json['status'] == 'Halt' or my_json['status'] == 'Error':
        abandon()	# Bail, there may be a good reason for the status
    if my_json['status'] == 'Drain':
        return	# Go on to next phase
    #
    # Look for outstanding jobs: status= NERSCRunning, count=NC
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetfindbundles + mangle('status=\"NERSCRunning\"'))
    outp, erro, code = getoutputerrorsimplecommand(geturl, 5)
    if int(code) != 0:
        print('Nrun check', str(outp))
        print(str(erro))
        print(str(code))
        return
        # Failed to get information.  It's a waste of time trying to
        # set an error when there are network problems
    #  if NC =0, return, to next phase
    #  abandon if fails to get info
    bundleJobJson = json.loads(singletodouble(outp.decode("utf-8")))
    numberJobs = len(bundleJobJson)
    if numberJobs == 0:
        if DEBUGIT:
            print('No jobs to do')
        return		# skip to the next phase
    #
    #
    #  slurm query for running jobs
    command = [squeue, '-h', '-o', '\"%.18i %.8j %.2t %.10M %.42k %R\"', '-M', 'escori', '-u', 'icecubed']
    outp, erro, code = getoutputerrorsimplecommand(command, 2)
    if int(code) != 0:
        print('Step 3', str(outp), str(erro), str(code))
        return		# Why didn't we get an answer?  Try again later
    #
    lines = str(outp).splitlines()
    # WARNING:  If the new rucio interface submits jobs under icecubed too, I'll need
    #  to count just _my_ jobs and not rely on the total number.
    #   if count of running jobs is equal to NC, return
    # Note:  There is a header line saying "CLUSTER: escori" so we always get N+1 lines
    if len(lines) > numberJobs:
        print('Step 5: # lines=', len(lines))
        return		# Everything is still running, nothing finished
    #
    if DEBUGIT:
    for bjson in bundleJobJson:
        # Check that either:
        #   a) the bundle name is in the slurm list
        #   b) the bundle name is in a completed log file
        # THIS ASSUMES THAT THE LOG FILE INCLUDES THE BUNDLE NAME!
        logfiles, logerr, logstat = getoutputerrorsimplecommand(['ls', logdir], 1)
        if int(logstat) != 0:
            print(logdir + ' access timed out')
            return	# Why didn't we get an answer, since filesystem is there?
        loglines = logfiles.decode("utf-8").splitlines()
        #
        foundfile = ''
        barename = bjson['idealName'].split('/')[-1]
        for aline in loglines:
            line = str(aline)
            if barename in line:
                foundfile = line
        # ??? No log file ???
        if foundfile == '':
            print('No log file found for ', bjson['idealName'])	# log this in email
            flagBundleError(bjson['bundleStatus_id'])
            return	# Something is gravely wrong
        # OK, open the log file
        try:
            filepointer = open(logdir + '/' + foundfile, 'r')
            filelines = filepointer.readlines()
            filepointer.close()
        except:
            filelines = []
        if len(filelines) == 0:
            print(bjson['idealName'], foundfile, 'is empty')   # log this in email
            flagBundleError(bjson['bundleStatus_id'])
            return      # Something is gravely wrong
        # Parse the log file for the size of the file in HPSS
        foundsize = -1
        for fline in filelines:
            if barename in fline and ' icecubed ' in fline:
                fwords = fline.split()
                if len(fwords) < 5:
                    print('Wrong line in file', fline)
                else:
                    try:
                        foundsize = int(fwords[4])
                    except:
                        print('Failed to get 5th word as int', fwords)
                        abandon()
        if foundsize != int(bjson['size']):
            print(bjson['idealName'], foundsize, 'is not ', str(bjson['size']))
            flagBundleError(bjson['bundleStatus_id'])
            return      # Something is puzzlingly wrong
        # If here, the job is done and the hpss size matches the expected 
        #  log NERSCClean for this file
        #  delete the scratch file
        #  move the old slurm log file to OLD
        #  nextfile
        posturl = copy.deepcopy(basicposturl)
        sqlcom = 'UPDATE BundleStatus SET status=\"NERSCClean\" WHERE bundleStatus_id={}'.format(bjson['bundleStatus_id'])
        posturl.append(targetupdatebundle + mangle(sqlcom))
        outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
        if int(code) != 0:
            # Try again
            print('First try', str(outp), str(erro), str(code))
            outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
            if int(code) != 0:
                print('code failure for ', bjson['bundleStatus_id'])
                flagBundleError(bjson['bundleStatus_id'])
                return      # Something is puzzlingly wrong
        if len(outp) == 0:
            command = [mv, logdir + '/' + foundfile, logdir + '/OLD/']
            noutp, nerro, ncode = getoutputerrorsimplecommand(command, 1)
            if int(ncode) != 0:
                noutp, nerro, ncode = getoutputerrorsimplecommand(command, 1)
                if int(ncode) != 0:
                    print('Cannot move ', foundfile)
                    continue
            command = [rm, SCRATCHROOT + bjson['idealName']]
            noutp, nerro, ncode = getoutputerrorsimplecommand(command, 1)
            if int(ncode) != 0:
                noutp, nerro, ncode = getoutputerrorsimplecommand(command, 1)
                if int(ncode) != 0:
                    print('Cannot rm ', SCRATCHROOT + bjson['idealName'])
                    continue
        else:   # if str(outp) is not OK
            print('Not OK??', str(outp))
            # We had a communication error?
            outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
            if str(outp) != 'OK':
                cerr = 'Problem executing ' + posturl[-1]
                print(cerr)
                posturl = copy.deepcopy(basicposturl).append(targetupdateerror + mangle(cerr))
                outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
                # If I'm getting communication errors on this too, nothing to do
                return
    return

# Look for files globus-copied into NERSC (PushDone)

def Phase3():
    #
    # Look for files with PushDone as the status, pull all info
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetfindbundles + mangle('status = \"PushDone\"'))
    listofbundles = getoutputsimplecommandtimeout(geturl, 30)
    if len(listofbundles) == 0:
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
    try:
        my_json = json.loads(singletodouble(listofbundles.decode("utf-8")))
    except:
        print('Phase3 json fail', listofbundles)
        abandon()
    for bundle in my_json:
        idealName = bundle['idealName']
        size = int(bundle['size'])
        key = bundle['bundleStatus_id']
        scratchname = SCRATCHROOT + idealName
        print(scratchname)
        command = ['ls', '-go', scratchname]
        outp, erro, code = getoutputerrorsimplecommand(command, 5)
        badFlag = False
        if int(code) != 0:
            badFlag = True
        else:
            ssize = int(outp.split()[2])
            if ssize != size:
                badFlag = True
        if badFlag:
            print('badFlag for', str(key))
            flagBundleError(key)
            abandon()
        barename = scratchname.split('/')[-1]
        command = [sbatch, 
                   '--comment=\"' + scratchname + '\"',
                   '-o', '/global/homes/i/icecubed/SLURMLOGS/slurm-' + barename + '-%j.out',
                   '/global/homes/i/icecubed/SLURMLOGS/xfer_put.sh', scratchname]        
        outp, erro, code = getoutputerrorsimplecommand(command, 5)
        if int(code) != 0:
            badFlag = True
        # I may check the code in more detail later
        if badFlag:
            print('badFlag2 for', str(key))
            flagBundleError(key)
            abandon()
        flagBundleRunning(key)
        activeSlurm = activeSlurm + 1
        if activeSlurm >= SLURMCUT:
            return	# Cannot launch any more
    #
    return

############
# MAIN PROGRAM
print('Starting')
Phase0()
print('done w/ 0: Token negotiation')
Phase1()
print('done w/ 1:  Checking for problems')
Phase2()
print('done w/ 2:  Checking for complete copy to HPSS')
Phase3()
print('done w/ 3:  Checking for files to be copied')
release()
