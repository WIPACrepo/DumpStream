import sys
# IMPORT_utils.py
# Assumes "import sys"
import datetime
import site
import json
import urllib.parse
import subprocess
import copy
import os
import uuid

# IMPORT CODE_utils.py
#####
# Define some constants
REPLACESTRING = '+++'
REPLACENOT = '==='
REPLACECURLLEFT = '+=+=+'
REPLACECURLRIGHT = '=+=+='
NERSCSTATI = ['Run', 'Halt', 'DrainNERSC', 'Error']
LOCALSTATI = ['Run', 'Halt', 'Drain', 'Error']
BUNDLESTATI = ['Untouched', 'JsonMade', 'PushProblem', 'PushDone',
               'NERSCRunning', 'NERSCDone', 'NERSCProblem', 'NERSCClean',
               'LocalDeleted', 'LocalFilesDeleted', 'Abort', 'Retry',
               'RetrieveRequest', 'RetrievePending', 'ExportReady',
               'Downloading', 'DownloadDone', 'RemoteCleaned']
DEBUGPROCESS = False
# WARN if free scratch space is low
FREECUTLOCAL = 50000000
FREECUTNERSC = 500
# How long can we wait before halting if NERSC_Client isn't running?
# For now, call it 2 hours
WAITFORNERSCAFTER = 120
# How many slurm jobs can go at once?
SLURMCUT = 14
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
targetfindbundlesin = curltargethost + 'bundles/specifiedin/'
targetfindbundleslike = curltargethost + '/bundles/specifiedlike/'
targettaketoken = curltargethost + 'nersctokentake/'
targetreleasetoken = curltargethost + 'nersctokenrelease/'
targetupdateerror = curltargethost + 'nersccontrol/update/nerscerror/'
targetnerscpool = curltargethost + 'nersccontrol/update/poolsize/'
targetnerscinfo = curltargethost + 'nersccontrol/info/'
targetsetnerscstatus = curltargethost + '/nersccontrol/update/status/'
targettokeninfo = curltargethost + 'nersctokeninfo'
targetdumpinfo = curltargethost + 'dumpcontrol/info'
targetheartbeatinfo = curltargethost + 'heartbeatinfo/'
targetupdatebundle = curltargethost + 'updatebundle/'
targetupdatebundlestatus = curltargethost + 'updatebundle/status/'
targetupdatebundleerr = curltargethost + 'updatebundleerr/'
targetaddbundle = curltargethost + 'addbundle/'
targetsetdumpstatus = curltargethost + '/dumpcontrol/update/status/'
targetsetdumppoolsize = curltargethost + '/dumpcontrol/update/poolsize/'
targetsetdumperror = curltargethost + '/dumpcontrol/update/bundleerror/'
targettree = curltargethost + '/tree/'
targetuntouchedall = curltargethost + '/bundles/alluntouched/'
targetupdatebundlestatusuuid = curltargethost + '/updatebundle/statusuuid/'
targetbundlestatuscount = curltargethost + '/bundles/statuscount/'
targetbundlesworking = curltargethost + '/bundles/working'
targetbundleinfobyjade = curltargethost + '/bundles/infobyjade/'

targetdumpingstate = curltargethost + '/dumping/state'
targetdumpingcount = curltargethost + '/dumping/state/count/'
targetdumpingnext = curltargethost + '/dumping/state/nextaction/'
targetdumpingstatus = curltargethost + '/dumping/state/status/'
targetdumpingpoledisk = curltargethost + '/dumping/poledisk'
targetdumpingpolediskslot = curltargethost + '/dumping/poledisk/infobyslot/'
targetdumpingpolediskuuid = curltargethost + '/dumping/poledisk/infobyuuid/'
targetdumpingpolediskid = curltargethost + '/dumping/poledisk/infobyid/'
targetdumpingpolediskstart = curltargethost + '/dumping/poledisk/start/'
targetdumpingpolediskdone = curltargethost + '/dumping/poledisk/done/'
targetdumpingpolediskloadfrom = curltargethost + '/dumping/poledisk/loadfrom/'
targetdumpingpoledisksetstatus = curltargethost + '/dumping/poledisk/setstatus/'
targetdumpingdumptarget = curltargethost + '/dumping/dumptarget'
targetdumpingsetdumptarget = curltargethost + '/dumping/dumptarget/'
targetdumpingoldtargets = curltargethost + '/dumping/olddumptarget/'
targetdumpingslotcontents = curltargethost + '/dumping/slotcontents'
targetdumpingsetslotcontents = curltargethost + '/dumping/slotcontents/'
targetdumpingwantedtrees = curltargethost + '/dumping/wantedtrees'
targetdumpingsetwantedtree = curltargethost + '/dumping/wantedtrees/'
targetdumpinggetactive = curltargethost + '/dumping/activeslots'
targetdumpinggetwaiting = curltargethost + '/dumping/waitingslots'
targetdumpinggetwhat = curltargethost + '/dumping/whatslots'
targetdumpinggetexpected = curltargethost + '/dumping/expectedir/'
targetdumpingcountready = curltargethost + '/dumping/countready'

targetdumpinggetreadydir = curltargethost + '/dumping/readydir'
targetdumpingnotifiedreadydir = curltargethost + '/dumping/notifiedreadydir/'
targetdumpingenteredreadydir = curltargethost + '/dumping/enteredreadydir/'
targetdumpingdonereadydir = curltargethost + '/dumping/donereadydir/'

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
        'NERSCProblem', 'NERSCClean', 'LocalDeleted', 'LocalFilesDeleted', 'Abort', 'Retry']

PoleDiskStatusOptions = ['New', 'Inventoried', 'Dumping', 'Done', 'Removed', 'Error']
DumperStatusOptions = ['Idle', 'Dumping', 'Inventorying', 'Error']
DumperNextOptions = ['Dump', 'Pause', 'DumpOne', 'Inventory']

# The backplane limit is about 2 Pole disks dumping at once.
DUMPING_LIMIT = 2
# 3 days worth of minutes
DUMPING_TIMEOUT = 4320
# Where do log files go?
DUMPING_LOG_SPACE = '/tmp/'
# Where do scripts live?
DUMPING_SCRIPTS = '/opt/i3admin/dumpscripts/'

# String manipulation stuff
def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING).replace('!', REPLACENOT)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/').replace(REPLACENOT, '!')

def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace(r'\,', ',').replace('@', ' ').replace(REPLACENOT, '!').replace(REPLACECURLLEFT, '{').replace(REPLACECURLRIGHT, '}')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', r'\,').replace(' ', '@').replace('!', REPLACENOT).replace('{', REPLACECURLLEFT).replace('}', REPLACECURLRIGHT)

def tojsonquotes(strFromPost):
    # Turn single into double quotes
    return strFromPost.replace("\'", "\"")

def fromjsonquotes(strFromPost):
    # Turn double into single quotes.  Won't use it much
    # here, but the remote jobs that feed this will
    return strFromPost.replace("\"", "\'")

def singletodouble(stringTo):
    return stringTo.replace('\'', '\"')

# timeout is in seconds
def getoutputerrorsimplecommand(cmd, Timeout):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate(Timeout)
        returncode = proc.returncode
        return output.decode('utf-8'), error, returncode
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


####
# Test parse byte-stream of list of dicts back into a list of strings
# each of which can be unpacked later into a dict
def stringtodict(instring):
    if len(instring) <= 1:
        return []
    countflag = 0
    initial = -1
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


def globusjson(localuuid, localdir, remotesystem, idealdir): 
    outputinfo = '{\n'
    outputinfo = outputinfo + '  \"component\": \"globus-mirror\",\n'
    outputinfo = outputinfo + '  \"version\": 1,\n'
    outputinfo = outputinfo + '  \"referenceUuid\": \"{}\",\n'.format(localuuid)
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

# Set a bundle's status
def flagBundleStatus(key, newstatus):
    if str(newstatus) not in BUNDLESTATI:
        return 'Failure', newstatus + ' is not allowed', '1'
    fbposturl = copy.deepcopy(basicposturl)
    comstring = mangle(str(newstatus) + ' ' + str(key))
    fbposturl.append(targetupdatebundlestatus + comstring)
    foutp, ferro, fcode = getoutputerrorsimplecommand(fbposturl, 15)
    if len(foutp) > 0:
        print('Failure in updating Bundlestatus to ' + str(newstatus)
              + 'for key ' + str(key) + ' : ' + str(foutp))
    return foutp, ferro, fcode

#
def deltaT(oldtimestring):
    current = datetime.datetime.now()
    try:
        oldt = datetime.datetime.strptime(oldtimestring, '%Y-%m-%d %H:%M:%S')
        difference = current - oldt
        delta = int(difference.seconds/60 + difference.days*60*24)
    except:
        delta = -1
    return delta
#

USEHEARTBEATS = False

########################################################
# Main

# optional argument : dups

NagiosError = ''
NagiosWarning = ''
THRESHOLDS = {'Untouched':10, 'NERSCProblem':0, 'NERSCClean':20, 'LocalDeleted':20, 'unstaged':20}
nstats = 'DB'

####
# NERSC status   
geturl = copy.deepcopy(basicgeturl)
geturl.append(targetnerscinfo)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' NERSC'
else:
    try:
        my_json = json.loads(singletodouble(outp))
        if str(my_json['status']) == 'Error':
            NagiosError = NagiosError + 'NERSC|'
        if int(deltaT(str(my_json['lastChangeTime']))) > 18000:
            NagiosWarning = NagiosWarning + 'NERSCLate|'
    except:
        nstats = nstats + ' NERSC'


####
# local status   
geturl = copy.deepcopy(basicgeturl)
geturl.append(targetdumpinfo)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' Bundle'
else:
    try:
        my_json = json.loads(singletodouble(outp))
        if str(my_json['status']) == 'Error':
            NagiosError = NagiosError + 'BundleScanner|'
        if int(deltaT(str(my_json['lastChangeTime']))) > 18000:
            NagiosWarning = NagiosWarning + 'BundleScannerLate|'
    except:
        nstats = nstats + ' Bundle'

####
# Disk dumping status
geturl = copy.deepcopy(basicgeturl)
geturl.append(targetdumpingstate)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' Dumping'
else:
    try:
        my_json = json.loads(singletodouble(outp))
        myj = my_json[0]
        if str(myj['status']) == 'Error':
            NagiosError = NagiosError + 'Dumper|'
    except:
        nstats = nstats + ' Dumping'

####
# FullDirectories available

geturl = copy.deepcopy(basicgeturl)
geturl.append(targetdumpingcountready)
outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' FullDir'
else:
    try:
        my_json = json.loads(singletodouble(outp))[0]
        if int(my_json['unstaged']) > THRESHOLDS['unstaged']:
            NagiosWarning = NagiosWarning + 'FDunstaged|'
    except:
        nstats = nstats + ' FullDir'


####
# How many bundles have each status?
# I will probably get fancier later.  For now, just this.
for opt in THRESHOLDS:
    if opt == 'unstaged':
        continue
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetbundlestatuscount + mangle(opt))
    outp, erro, code = getoutputerrorsimplecommand(geturl, 1)
    #print(outp)
    if int(code) != 0:
        nstats = nstats + ' ' + opt
    else:
        try:
            my_json = json.loads(singletodouble(outp))
            js = my_json[0]
            if int(js['COUNT(*)']) > THRESHOLDS[opt]:
                NagiosWarning = NagiosWarning + ' ' + opt
        except:
            nstats = nstats + ' ' + opt

print(NagiosError)
print(NagiosWarning)
if nstats != 'DB':
    print(nstats)
