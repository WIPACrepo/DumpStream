# DumpControl.py.base
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
#
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

##########
# Utilities:
#  1) Check a slot
#    a) Is it readable?
#    b) What is its UUID?  The .json file tells us
#    c) Get the wanted trees and parse for the top directories
#    d) Organize the groups by top directory
#    e) Look in those top directories to find the year(s) for each
#    f) Use those lists and those years to create a new list of
#       rsync sources, and return this and the UUID
#  2) Create a rsync bash script using the UUID and rsync sources
#    a) New file w/ UUID.sh name and a list
#    b) Submit this (do I want the process ID too?)



#################################################################
# INVENTORY CODE
#
###
# Give top directories from a list
def GiveTops(desiredtrees):
    if len(desiredtrees) <= 0:
        return []
    toplist = []
    for tree in desiredtrees:
        tip = tree.split('/')[0]
        if tip not in toplist:
            toplist.append(tip)
    return toplist

###
# Return the target directory for copying "to"
def GiveTarget():
    gtgeturl = copy.deepcopy(basicgeturl)
    gtgeturl.append(targetdumpingdumptarget)
    gtoutp, gterro, gtcode = getoutputerrorsimplecommand(gtgeturl, 1)
    if int(gtcode) != 0 or 'FAILURE' in str(gtoutp):
        print('Get Target directory failed', gtoutp)
        sys.exit(0)
    dump_json = json.loads(singletodouble(gtoutp))
    try:
        directory = dump_json[0]['target']
    except:
        print('Cannot unpack', dump_json)
        sys.exit(0)
    return directory

###
#  Count the number of files in the specified directory and compare
#   with the expected number
def CountFilesInDir(cfidname, cfidexpectedcount):
    cfidcommand = ['/bin/ls', cfidname]
    cfidoutp, cfiderro, cfidcode = getoutputerrorsimplecommand(cfidcommand, 1)
    if int(cfidcode) != 0:
        print('CountFilesInDir failed to see', cfidname, cfiderro)
        sys.exit(0)
    #
    return len(cfidoutp.split()) == cfidexpectedcount

####
# Return the subdirectory names

###
# Inventory a slot.  Pass it the json for the slot (has old info)
# and the list of trees we want to archive.  YEAR must be
# replaced with the actual year(s) found
def InventoryOneFull(slotlocation):
    # First find out what trees we want to read
    i1geturl = copy.deepcopy(basicgeturl)
    i1geturl.append(targetdumpingwantedtrees)
    i1outp, i1erro, i1code = getoutputerrorsimplecommand(i1geturl, 1)
    if int(i1code) != 0 or 'FAILURE' in str(i1outp):
        print('Get trees failure', i1geturl, i1outp)
        sys.exit(0)
    desiredtrees = []
    my_json = json.loads(singletodouble(i1outp))
    for js in my_json:
        h = js['dataTree']
        desiredtrees.append(h)

    #
    # If no trees to read, why bother?
    if len(desiredtrees) <= 0:
        return []
    #
    toplist = GiveTops(desiredtrees)
    #
    # Note that even if the disk is not mounted, the mountpoint
    #  is still there, and will be "readable" if not populated
    # Bail if it is empty--probably not mounted
    #
    command = ['/bin/ls', slotlocation]
    i2outp, i2erro, i2code = getoutputerrorsimplecommand(command, 1)
    if int(i2code) != 0 or len(i2outp) <= 1:
        print('Cannot read the disk in', slotlocation)
        return []	# Do I want to set an error?
    answer = str(i2outp)
    for toplevel in answer.split():
        #print(toplevel)
        if len(toplevel.split('-')) == 5:
            diskuuid = toplevel
    #
    # Generate the list of directories to rsync
    dirstoscan = []
    for tip in toplist:
        if tip in answer.split():
            command = ['/bin/ls', slotlocation + '/' + tip]
            i3outp, i3erro, i3code = getoutputerrorsimplecommand(command, 1)
            if int(i3code) != 0:
                continue	# May not be present, do not worry about it
            tipyearlist = []
            for y in i3outp.split():
                tipyearlist.append(y)
            for dt in desiredtrees:
                if tip in dt:
                    words = dt.split('YEAR')
                    for y in tipyearlist:
                        dirstoscan.append(words[0] + y + words[1])
    #for j in dirstoscan:
    #    print(j)
    return [diskuuid, dirstoscan]

####
# Get the UUID, if this is readable, for use with slot
# management
def InventoryOne(slotlocation):
    #
    command = ['/bin/ls', slotlocation]
    i1outp, i1erro, i1code = getoutputerrorsimplecommand(command, 1)
    if int(i1code) != 0 or len(i1outp) <= 1:
        print('Cannot read the disk in', slotlocation)
        return []	# Do I want to set an error?
    answer = str(i1outp)
    diskuuid = ''
    for toplevel in answer.split():
        #print(toplevel)
        if len(toplevel.split('-')) == 5:
            diskuuid = toplevel
    return diskuuid

####
# Set the Poleid for a given slot#
def SetSlotsPoleID(slotnum, poleidnum):
    case = mangle(str(slotnum) + ' ' + str(poleidnum))
    ssposturl = copy.deepcopy(basicposturl)
    ssposturl.append(targetdumpingsetslotcontents + case)
    s1outp, s1err, s2code = getoutputerrorsimplecommand(ssposturl, 1)
    if len(s1outp) != 0 or s2code != 0:
        print('SetSlotsPoleID failed', case)
        sys.exit(0)


####
# Go through all the slots and see what the UUID is in them
# Connect information between SlotContents and PoleDisk
#  entries
def InventoryAll():
    i1geturl = copy.deepcopy(basicgeturl)
    i1geturl.append(targetdumpingslotcontents)
    i1outp, i1erro, i1code = getoutputerrorsimplecommand(i1geturl, 1)
    if int(i1code) != 0:
        print('SlotContents failure', i1geturl, i1outp)
        sys.exit(0)
    my_json = json.loads(singletodouble(i1outp))
    i1geturl = copy.deepcopy(basicgeturl)
    i1geturl.append(targetdumpingdumptarget)
    i1outp, i1erro, i1code = getoutputerrorsimplecommand(i1geturl, 1)
    if int(i1code) != 0 or 'FAILURE' in str(i1outp):
        print('get dump target failure', i1geturl, i1outp)
        sys.exit(0)
    targetareaj = json.loads(singletodouble(str(i1outp)))[0]
    targetarea = targetareaj['target']
    #
    for js in my_json:
        # First check if the slot is disabled
        if js['poledisk_id'] < 0:
            continue
        # Now inventory the slot--get the UUID of the disk, if available
        diskuuid = InventoryOne(js['name'])
        if diskuuid == '' or diskuuid == []:
            print('Got nothing for', js['slotnumber'])
            SetSlotsPoleID(js['slotnumber'], 0)
            continue
        #
        # Link SlotContents to the new PoleDisk
        try:
            stuffforpd = '{\'diskuuid\':\'' + diskuuid + '\', \'slotnumber\':' + str(js['slotnumber']) + ', \'targetArea\':\'' + targetarea + '\', \'status\':\'Inventoried\'}'
        except:
            print('FAILS', diskuuid, targetarea)
            sys.exit(0)
        i1posturl = copy.deepcopy(basicposturl)
        i1posturl.append(targetdumpingpolediskloadfrom + mangle(stuffforpd))
        i2outp, i2erro, i2code = getoutputerrorsimplecommand(i1posturl, 1)
        if int(i2code) != 0 or 'FAILURE' in str(i2outp) or '404 Not Found' in str(i2outp):
            print('Load new PoleDisk failed', i1posturl)
            print(i2outp, i2erro, i2code)
            print(stuffforpd)
            sys.exit(0)
        #
        # OK, now I want to read back what I wrote so I get the new poledisk_id
        i2geturl = copy.deepcopy(basicgeturl)
        i2geturl.append(targetdumpingpolediskuuid + mangle(diskuuid))
        i2outp, i2erro, i2code = getoutputerrorsimplecommand(i2geturl, 1)
        if len(i2outp) == 0 or 'FAILURE' in str(i2outp):
            print('Retrive PoleDisk info failed', i2geturl, i2outp, i2erro, i2code)
            sys.exit(0)
        soutp = i2outp
        try:
            djson = json.loads(singletodouble(soutp))
            js = djson[0]
            cslot = str(js['slotnumber'])
            cpole = str(js['poledisk_id'])
        except:
            print('Retrieve of new poledisk_id failed', soutp)
            sys.exit(0)
        SetSlotsPoleID(cslot, cpole)

    return

#########################################################
# JOB CHECK CODE
def JobInspectAll():
    # First get a list of all the nominally active slots
    jigeturl = copy.deepcopy(basicgeturl)
    jigeturl.append(targetdumpinggetactive)
    jioutp, jierro, jicode = getoutputerrorsimplecommand(jigeturl, 1)
    if int(jicode) != 0:
        print('JobInspectAll: active slots check failure', jigeturl, jioutp)
        sys.exit(0)
    try:
        my_json = json.loads(singletodouble(jioutp))
    except:
        print('JobInspectAll:  cannot parse ', jioutp)
        sys.exit(0)
    # Load the relevant info up:
    expected = []
    for js in my_json:
        expected.append([js['diskuuid'], js['slotnumber'], js['dateBegun'], js['poledisk_id'], js['name']])
    # Suppose expected is empty and the list of jobs isn't?
    # Flag an error
    #
    #  I expect that the active slot info will be a superset
    #  of the jobs found.  If they match, and the count is
    #  equal to or higher than DUMPING_LIMIT,
    #  don't bother doing anything.
    #  Inspecting the dateBegun vs today might be useful, though
    #
    # Now find out what jobs are active
    #commandj = ['/usr/bin/ps', 'aux']
    commandj = ['/bin/ps', 'aux']
    listing, jerro, jcode = getoutputerrorsimplecommand(commandj, 1)
    if int(jcode) != 0:
        print('JobInspectAll: pstree failed', joutp, jerro, jcode)
        sys.exit(0)
    candidate = []
    wlisting = listing.split()
    for line in wlisting:
        if 'DUMPING' in line:
            candidate.append(line)
    # Inspect if the expected jobs are still running
    matching = []
    notmatched = []
    for v in expected:
        found = False
        for c in candidate:
            if v[0] in c:	# The uuid.  Will there be truncation?
                matching.append(v)
                found = True
                continue
        if not found:
            notmatched.append(v)
    # expected = DB query says these may be still active
    # candidate = jobs actively running
    # matching = jobs running that are in DB
    # notmached = jobs not running that are in DB (=done?)
    return expected, candidate, matching, notmatched

####
# Check if the log space is getting full
def CheckLogSpace():
    #commandc = ['/usr/bin/df', '-h', DUMPING_LOG_SPACE]
    commandc = ['/bin/df', '-h', DUMPING_LOG_SPACE]
    coutp, cerro, ccode = getoutputerrorsimplecommand(commandc, 1)
    if int(ccode) != 0:
        print('CheckLogSpace failed', coutp, cerro, ccode, DUMPING_LOG_SPACE)
        sys.exit(0)
    percent = int(coutp.split()[-2].split('%')[0])
    return percent < 90

####
# Flag completed jobs as needed.  Sanity checking
def JobDecisionCompleted(notmatched):
    # Look for completed jobs and flag them
    # First sanity check
    donelist = []
    if len(notmatched) == 0:
        return donelist
    #
    for jdone in notmatched:
        # First make sure nothing is wrong
        # I expect a script in DUMPING_LOG_SPACE/DUMPING_${UUID} and a log file
        # in DUMPING_LOG_SPACE/DUMPING_${UUID}.log
        jid = jdone[3]
        tentative = DUMPING_LOG_SPACE + 'DUMPING_' + jdone[0] + '.log'
        commandj = ['/usr/bin/tail', '-n1', tentative]
        answerline, jerro, jcode = getoutputerrorsimplecommand(commandj, 1)
        if int(jcode) != 0:
            print('JobDecisionCompleted failed to tail', tentative, 'X')
            sys.exit(0)
        #
        # If the file is still empty, presumably it is not yet done
        if len(answerline) == 0:
            continue
        #
        # Sanity checking--expect "SUMMARY 5 5" or however many rsyncs were done
        summaryinfo = answerline.split()
        if summaryinfo[0] != 'SUMMARY':
            continue
        try:
            numtried = int(summaryinfo[1])
            numsucceeded = int(summaryinfo[2])
        except:
            print('JobDecisionCompleted summary info line corrupt for', tentative, summaryinfo)
            print('It may have crashed.  Rerun the rsync?', answerline, 'X')
            sys.exit(0)
        if numtried != numsucceeded:
            print('JobDecisionCompleted summary line info shows problems for', tentative, summaryinfo)
            print('Rerun the rsync?', answerline, 'X')
            sys.exit(0)
        jdposturl = copy.deepcopy(basicposturl)
        jdposturl.append(targetdumpingpolediskdone + mangle(str(jid)))
        jdoutp, jderro, jdcode = getoutputerrorsimplecommand(jdposturl, 1)
        if int(jdcode) != 0 or 'FAILURE' in str(jdoutp):
            print('JobDecisionCompleted: Set status of PoleDisk failed', jid)
            sys.exit(0)
        donelist.append(jdone)
    # Done flagging completed jobs
    return donelist

###
# Set the PoleDisk status
def SetPoleDiskStatus(spid, sstatus):
    if sstatus not in PoleDiskStatusOptions:
        print('SetPoleDiskStatus:  bad status', sstatus)
        sys.exit(0)
    sarg = mangle(str(spid) + ' ' + sstatus)
    spposturl = copy.deepcopy(basicposturl)
    spposturl.append(targetdumpingpoledisksetstatus + sarg)
    spoutp, sperro, spcode = getoutputerrorsimplecommand(spposturl, 1)
    if int(spcode) != 0 or 'FAILURE' in str(spoutp):
        print('SetPoleDiskStatus: Set status failed', sarg)
        sys.exit(0)

###
# Get the expected count from the DB
def GetExpectedCount(trialdirname):
    #  This checks whether the fragment matches anything in the DB
    gegeturl = copy.deepcopy(basicgeturl)
    gegeturl.append(targetdumpinggetexpected + mangle(trialdirname))
    geoutp, geerro, gecode = getoutputerrorsimplecommand(gegeturl, 1)
    if gecode != 0 or len(geoutp) <= 0:
        return -1
    try:
        gejson = json.loads(singletodouble(geoutp))
    except:
        print('No expected counts for directory', trialdirname)
        return -1
    try:
        genumber = gejson[0]['number']
    except:
        return -1
    try:
        gen = int(genumber)
    except:
        gen = -1
    return gen
    

###
# Determine whether any of the directories are now full.  Return the
# target directory in question if so
def DirectoryCheckFull(donelist):
    # Each entry in the array is an array with [uuid,slot#,datebegun,poledisk_id,slotname]
    dcok = []
    if len(donelist) <= 0:
        return
    dtargetdir = GiveTarget()
    for packet in donelist:
        # This repeats an earlier disk access!
        dstuff = InventoryOneFull(packet[4])
        for tentativedir in dstuff[1]:
            # It should, in theory not have been full before, and thus
            #  there should not be anything in FullDirectories.  But,
            #  to be on the safe side, check before registering it
            numberexpect = GetExpectedCount(tentativedir)
            if numberexpect < 0:
                print('DirectoryCheckFull has nothing for', tentativedir)
                continue
            dtarg = dtargetdir + '/' + tentativedir
            if CountFilesInDir(dtarg, numberexpect):
                dcok.append(dtarg)
                dcposturl = copy.deepcopy(basicposturl)
                dcposturl.append(targetdumpingenteredreadydir + mangle(dtarg))
                dcoutp, dcerro, dccode = getoutputerrorsimplecommand(geposturl, 1)
                if dccode != 0 or 'FAILURE' in str(dcoutp):
                    print('DirectoryCheckFull could not load', dtarg)
                    continue
    return

###
# Load

####
# Decide whether a new job is needed, or whether an old job is done
#  Do some cleanup too
def JobDecision(dumperstatus, jdumpnextAction):
    # Inspect what's out there
    expected, candidate, matching, notmatched = JobInspectAll()
    # Sanity check
    if len(expected) < len(candidate):
        print('JobDecision: we have more jobs running than expected!', len(expected), len(candidate))
        sys.exit(0)
    #
    # Check on completed jobs
    donelist = JobDecisionCompleted(notmatched)
    #
    # Now look through the completed list and see what needs doing with
    # the directories it loaded.  It will push info into FullDirectories
    DirectoryCheckFull(donelist)
    #
    # Next see how long the current set of jobs has been taking
    for jdrunning in matching:
        dateBegun = datetime.datetime.strptime(str(jdrunning[2]), '%Y-%m-%d %H:%M:%S')
        # Since all the times are local, I can use something simple
        minutes = (datetime.datetime.today() - dateBegun).total_seconds() / 60
        if minutes > DUMPING_TIMEOUT:
            print('JobDecision:  job for', jdrunning[0], 'has run long')
            # Don't bail, this might be OK
    #
    # What should we be doing?
    if jdumpnextAction == 'Pause' or dumperstatus == 'Idle':
        return
    #
    # See if the jobcount is low enough to let me add another dump
    if len(candidate) >= DUMPING_LIMIT:
        return
    #
    # Got room for another...
    if not CheckLogSpace():
        print('JobDecision:  log space running short')
        return
    # Pick up the next one
    jdgeturl = copy.deepcopy(basicgeturl)
    jdgeturl.append(targetdumpinggetwaiting)
    jdoutp, jderro, jdcode = getoutputerrorsimplecommand(jdgeturl, 1)
    if int(jdcode) != 0 or 'FAILURE' in str(jdoutp):
        print('Get next undone disk failed', jdoutp)
        sys.exit(0)
    if len(jdoutp) <= 0:
        return		# Nothing left to do here
    my_json = json.loads(singletodouble(jdoutp))
    try:
        juuid = my_json[0]['diskuuid']
        jid = my_json[0]['poledisk_id']
        slotnumber = my_json[0]['slotnumber']
    except:
        print('JobDecision:  cannot unpack next disk', my_json)
        sys.exit(0)
    #
    #commandx = ['/usr/bin/cp', DUMPING_SCRIPTS + 'dumpscript', DUMPING_LOG_SPACE + 'DUMPING_' + str(juuid)]
    commandx = ['/bin/cp', DUMPING_SCRIPTS + 'dumpscript', DUMPING_LOG_SPACE + 'DUMPING_' + str(juuid)]
    xoutp, xerro, xcode = getoutputerrorsimplecommand(commandx, 1)
    if int(xcode) != 0:
        print('JobDecision: failed to copy to new script')
        sys.exit(0)
    jdgeturl = copy.deepcopy(basicgeturl)
    jdgeturl.append(targetdumpingslotcontents)
    jdoutp, jderro, jdcode = getoutputerrorsimplecommand(jdgeturl, 1)
    if int(jdcode) != 0:
        print('JobDecision: SlotContents failure', jdgeturl, jdoutp)
        sys.exit(0)
    sl_json = json.loads(singletodouble(jdoutp))
    for js in sl_json:
        if js['slotnumber'] == slotnumber:
            slotlocation = js['name']
            break
    returnstuff = InventoryOneFull(slotlocation)
    if len(returnstuff) < 2:
        # nothing here--not sure why
        SetPoleDiskStatus(jid, 'Error')
        return
    targetdir = GiveTarget()
    dirstoscan = returnstuff[1]
    if len(dirstoscan) == 0:
        # nothing we want here, call it done.
        SetPoleDiskStatus(jid, 'Done')
        return
    commandy = [DUMPING_SCRIPTS + 'submitdumper', DUMPING_LOG_SPACE + 'DUMPING_' + str(juuid)]
    for source in dirstoscan:
        commandy.append(slotlocation + '/' + source)
        # Create the target directory...
        reducedsource = os.path.dirname(source)
        commandy.append(targetdir + '/' + reducedsource)
    youtp, yerro, ycode = getoutputerrorsimplecommand(commandy, 1)
    if int(ycode) != 0:
        print('JobDecision: submitdumper failed ')
        sys.exit(0)
    #
    # Update PoleDisk info
    jdposturl = copy.deepcopy(basicposturl)
    jdposturl.append(targetdumpingpolediskstart + mangle(str(jid)))
    jdoutp, jderro, jdcode = getoutputerrorsimplecommand(jdposturl, 1)
    if int(jdcode) != 0 or 'FAILURE' in str(jdoutp):
        print('JobDecision:  update PoleDisk w/ start time failed', jdoutp)
        sys.exit(0)
    # If we're running in DumpOne mode, Pause 
    if jdumpnextAction == 'DumpOne':
        DumperSetState('Pause')
    #
    #
    return

####
# State of the Dumper: query and set routines
def DumperTodo():
    dtgeturl = copy.deepcopy(basicgeturl)
    dtgeturl.append(targetdumpingstate)
    dtoutp, dterro, dtcode = getoutputerrorsimplecommand(dtgeturl, 1)
    if int(dtcode) != 0 or 'FAILURE' in str(dtoutp):
        print('Get Dumper state failed', dtoutp)
        sys.exit(0)
    # status, nextAction
    dump_json = json.loads(singletodouble(dtoutp))
    status = dump_json[0]['status']
    nextAction = dump_json[0]['nextAction']
    return status, nextAction
#DumperStatusOptions = ['Idle', 'Dumping', 'Inventorying', 'Error']
#DumperNextOptions = ['Dump', 'Pause', 'DumpOne', 'Inventory']
#
def DumperSetState(value):
    if value not in DumperStatusOptions:
        print('DumperSetState:  invalid state to set', value)
        sys.exit(0)
    dposturl = copy.deepcopy(basicposturl)
    dposturl.append(targetdumpingstatus + mangle(value))
    doutp, derro, dcode = getoutputerrorsimplecommand(dposturl, 1)
    if int(dcode) != 0 or 'FAILURE' in str(doutp):
        print('DumperSetState: failed to set', value, doutp, derro)
        sys.exit(0)
def DumperSetNext(value):
    if value not in DumperNextOptions:
        print('DumperSetNext:  invalid state to set', value)
        sys.exit(0)
    dposturl = copy.deepcopy(basicposturl)
    dposturl.append(targetdumpingnext + mangle(value))
    doutp, derro, dcode = getoutputerrorsimplecommand(dposturl, 1)
    if int(dcode) != 0 or 'FAILURE' in str(doutp):
        print('DumperSetNext: failed to set', value, doutp, derro)
        sys.exit(0)


###########
# MAIN
#
# Should we be active at all?
#  Note that Pause still lets us check whether old jobs have completed.
dumpstatus, dumpNextAction = DumperTodo()
if dumpNextAction == 'Inventory':
    # Do not start dumping immediately after inventory!
    InventoryAll()
    DumperSetState('Idle')
    dumpstatus = 'Idle'
    DumperSetNext('Pause')
    dumpnextAction = 'Pause'

# Note that the Idle status still allows us to check whether old
# jobs have completed.
if dumpstatus == 'Error' or dumpstatus == 'Inventorying':
    sys.exit(0)

if dumpNextAction == 'Dump' and dumpstatus == 'Idle':
    DumperSetState('Dumping')
    dumpstatus = 'Dumping'

jobinformation = JobInspectAll()
JobDecision(dumpstatus, dumpNextAction)
