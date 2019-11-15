# ManualBundle.py (.base)
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
# IMPORT_db.py 
# Assumes we have "import sys" as well
import pymysql

# IMPORT CODE_utils.py
#####
# Define some constants
REPLACESTRING = '+++'
REPLACENOT = '==='
NERSCSTATI = ['Run', 'Halt', 'DrainNERSC', 'Error']
LOCALSTATI = ['Run', 'Halt', 'Drain', 'Error']
BUNDLESTATI = ['Untouched', 'JsonMade', 'PushProblem', 'PushDone',
               'NERSCRunning', 'NERSCDone', 'NERSCProblem', 'NERSCClean',
               'LocalDeleted', 'LocalFilesDeleted', 'Abort', 'Retry']
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

# String manipulation stuff
def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING).replace('!', REPLACENOT)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/').replace(REPLACENOT, '!')

def unmangle(strFromPost):
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

# timeout is in seconds
def getoutputsimplecommandtimeout(cmd, Timeout):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #proc = subprocess.call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate(Timeout)
        if len(error) != 0:
            print('ErrorA:::', cmd, '::-::', error)
            return ""
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
    posturl = copy.deepcopy(basicposturl)
    comstring = mangle(str(newstatus) + ' ' + str(key))
    posturl.append(targetupdatebundlestatus + comstring)
    outp, erro, code = getoutputerrorsimplecommand(posturl, 15)
    if len(outp) > 0:
        print('Failure in updating Bundlestatus to ' + str(newstatus)
              + 'for key ' + str(key) + ' : ' + str(outp))
    return outp, erro, code

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
DBdatabase = None
DBcursor = None
######################################################
######
# DB connection established
def getdbgen():
    global DBdatabase
    global DBcursor
    try:
        # https://stackoverflow.com/questions/27203902/cant-connect-to-database-pymysql-using-a-my-cnf-file
        DBdatabase = pymysql.connect(read_default_file='~/.my.cnf',)
    except pymysql.OperationalError:
        print(['ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(1)
    except Exception:
        print(['ERROR: Generic failure to connect to MySQL archivedump database.', sys.exc_info()[0]])
        sys.exit(1)
    try:
        #DBcursor = DBdatabase.cursor()
        DBcursor = DBdatabase.cursor(pymysql.cursors.DictCursor)
    except pymysql.OperationalError:
        print(['ERROR: could not get cursor for database.'])
        sys.exit(1)
    except Exception:
        print(['ERROR: Generic failure to get cursor for database.', sys.exc_info()[0]])
        sys.exit(1)
    return

####
def returndbgen():
    return DBcursor

####
def closedbgen():
    DBcursor.close()
    DBdatabase.close()
    return

####
# Commit changes to DB specified
def doCommitDB():
    try:
        DBdatabase.commit()
    except pymysql.OperationalError:
        DBdatabase.rollback()
        print(['doCommitDB: ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(1)
    except Exception:
        DBdatabase.rollback()
        print(['doCommitDB: Failed to execute the commit'])
        sys.exit(1)



############################################
######  Execute a command.  Crash if it fails, otherwise return silently
def doOperationDB(dbcursor, command, string):
    try:
        dbcursor.execute(command)
        return
    except pymysql.OperationalError:
        print(['ERROR: doOperationDB could not connect to MySQL ', string, ' database.', command])
        sys.exit(1)
    except Exception:
        print(['ERROR: doOperationDB undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(1)
    return

############################################
######  Execute a command.  Crash if it fails, return False if it didn't work right, True if it was OK
def doOperationDBWarn(dbcursor, command, string):
    try:
        dbcursor.execute(command)
        return True
    except pymysql.OperationalError:
        print(['ERROR: doOperationDBWarn could not connect to MySQL ', string, ' database.', command])
        sys.exit(1)
    except pymysql.IntegrityError:
        print(['ERROR: doOperationDBWarn \"IntegrityError\", probably duplicate key', string, ' database.', sys.exc_info()[0], command])
        return False
    except Exception:
        print(['ERROR: doOperationDBWarn undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(1)
    return True

############################################
######  Execute a command, return the answer.  Or error messages if it failed
def doOperationDBTuple(dbcursor, command, string):
    try:
        dbcursor.execute(command)
        expectedtuple = dbcursor.fetchall()
        return expectedtuple		#Assume you know what you want to do with this
    except pymysql.OperationalError:
        return ['ERROR: doOperationDBTuple could not connect to MySQL ', string, ' database.', command]
    except Exception:
        return ['ERROR: doOperationDBTuple undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command]
    return [[]]

DEBUGIT = False
########################################################
# Define Phases for Main.  In this case, only the last phase
#  is relevant--the rest can be done on jade-lta.  This can
#  be done on a cobalt (~jbellinger/archivemonitor)
# This is NOT invoked automatically by cron, so some safety
#  features can be omitted:  e.g. whether the status is Run
#  or Halt or Drain.

def Phase5():
    #
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetfindbundles + mangle('NERSCClean'))
    answer = massage(getoutputsimplecommandtimeout(geturl, 1))
    if 'DOCTYPE HTML PUBLIC' in answer or 'FAILURE' in answer:
        print('Phase 5 failure with', geturl)
        return
    if len(answer) == 0:
        return	# Nothing to do
    jjanswer = json.loads(singletodouble(answer))
    numwaiting = len(jjanswer)
    # Sanity check
    if numwaiting <= 0:
        # This should not happen, but maybe the json isn't understood
        print('Phase 5 json is empty', str(answer))
        return
    for js in jjanswer:
        try:
            localname = js['localName']
        except:
            print('Phase 5: problem with getting info from', js)
            continue
        print('Candidate for removal', localname)
    shortanswer = input('OK to remove the above? y/Y ').lower()
    if len(shortanswer) <= 0:
        return			# Don't do anything
    if shortanswer[0] != 'y':
        return			# Don't do anything
    #
    for js in jjanswer:
        try:
            localname = js['localName']
        except:
            print('Phase 5: problem with getting info from', js)
            continue
        try:
            command = ['/usr/bin/ls', localname]
            outp, erro, code = getoutputerrorsimplecommand(command, 1)
            if int(code) == 0:
                command = ['/usr/bin/rm', localname]
                outp, erro, code = getoutputerrorsimplecommand(command, 1)
        except:
            print('Phase 5: I do not see the file, or else deleting it fails', localname)
            continue
        key = js['bundleStatus_id']
        outp, erro, code = flagBundleStatus(key, 'LocalDeleted')
        if len(outp) > 0:
            print('Phase 5: failed to set status=LocalDeleted for', localname, outp)
            continue 
    return

###############
# Main

Phase5()
# Phase5 does not work unless we have write access to the bundles'
#  scratch filesystem, and jade-lta does not.
