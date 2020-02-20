# Utils.py (.base)
''' Define a lot of constants and utility routines for my REST framework.
    Some wheel-reinventing here:  I would do things differently now '''
import sys
# IMPORT_utils.py
# Assumes "import sys"
import datetime
import json
import subprocess
import copy
import os
# IMPORT_db.py 
# Assumes we have "import sys" as well
import pymysql

# IMPORT CODE_utils.py
#####
# Define some constants.  A whole bunch of constants.
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
targetbundleget = curltargethost + '/bundles/get/'
targetbundlegetlike = curltargethost + '/bundles/getlike/'
targetbundlepatch = curltargethost + '/bundles/patch/'
targetallbundleinfo = curltargethost + '/bundles/allbundleinfo'

targetbundleactivediradd = curltargethost + '/bundles/gactive/add/'
targetbundleactivedirremove = curltargethost + '/bundles/gactive/remove/'
targetbundleactivedirfind = curltargethost + '/bundles/gactive/find/'
targetbundleactivedirclean = curltargethost + '/bundles/gactive/clean'

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
tdargetdumpingdonereadydir = curltargethost + '/dumping/donereadydir/'
tdargetdumpinghandedoffdir = curltargethost + '/dumping/handedoffdir/'

targetgluestatus = curltargethost + 'glue/status/'
targetglueworkload = curltargethost + 'glue/workload/'
targetglueworkupdate = curltargethost + 'glue/workupdate/'
targetglueworkcount = curltargethost + 'glue/workcount'
targetglueworkpurge = curltargethost + 'glue/workpurge'
targetgluetimeset = curltargethost + 'glue/timeset/'
targetgluetimediff = curltargethost + 'glue/timediff'
targetgluetoken = curltargethost + 'glue/token/'

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
# Where do master log files go?
DUMPING_MASTER_LOG_SPACE = '/opt/i3admin/shortlogs/'
# Where do scripts live?
DUMPING_SCRIPTS = '/opt/i3admin/dumpscripts/'
#
# MySQL database pointers
DBdatabase = None
DBcursor = None

# String manipulation stuff
def unslash(strWithSlashes):
    ''' Replace '/' with special string '''
    #+
    # Arguments:	string to be massaged
    # Returns:		string with '/' replaced with special string
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    return strWithSlashes.replace('/', REPLACESTRING).replace('!', REPLACENOT)

def reslash(strWithoutSlashes):
    ''' Replace special string with '/' '''
    #+
    # Arguments:	string to be restored
    # Returns:		string with special string replaced with '/'
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    return strWithoutSlashes.replace(REPLACESTRING, '/').replace(REPLACENOT, '!')

def unmangle(strFromPost):
    ''' Replace special tokens with the stuff that confuses curl '''
    #+
    # Arguments:        string to be restored
    # Returns:          string with special tokens restored to curl confusers (e.g. spaces)
    # Side Effects:     None
    # Relies on:        Nothing
    #-
    return strFromPost.replace(REPLACESTRING, '/').replace(r'\,', ',').replace('@', ' ').replace(REPLACENOT, '!').replace(REPLACECURLLEFT, '{').replace(REPLACECURLRIGHT, '}')

def mangle(strFromPost):
    ''' Replace stuff that confuses curl with special tokens '''
    #+
    # Arguments:        string to be made curl-friendly
    # Returns:          string with curl confusers (e.g. spaces) replace w/ tokens
    # Side Effects:     None
    # Relies on:        Nothing
    #-
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', r'\,').replace(' ', '@').replace('!', REPLACENOT).replace('{', REPLACECURLLEFT).replace('}', REPLACECURLRIGHT)

def tojsonquotes(strFromPost):
    ''' replace single with double quotes '''
    #+
    # Arguments:        string with possible single quotes
    # Returns:          string with double quotes only
    # Side Effects:     None
    # Relies on:        Nothing
    #-
    # Remote jobs will use this more than we will here.
    # Turn single into double quotes
    return strFromPost.replace("\'", "\"")

def fromjsonquotes(strFromPost):
    ''' replace with single quotes '''
    #+
    # Arguments:        string with possible double quotes
    # Returns:          string with single quotes only
    # Side Effects:     None
    # Relies on:        Nothing
    #-
    # Remote jobs will use this more than we will here.
    # Turn double into single quotes.  Won't use it much
    # here, but the remote jobs that feed this will
    return strFromPost.replace("\"", "\'")

def singletodouble(stringTo):
    ''' Replace single quotes with double quotes '''
    #+
    # Arguments:	string
    # Returns:		string with single quotes changed to double
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    return stringTo.replace('\'', '\"')

# timeout is in seconds
def getoutputerrorsimplecommand(cmd, Timeout):
    ''' workhorse interface for sub-process spawning '''
    #+
    # Arguments:	command (as an array of strings)
    #			timeout requested in seconds
    # Returns:		output from subprocess operation
    # 			stderr from subprocess operation
    # 			returncode from subprocess operation
    # Side Effects:	prints error if there was a failure
    #			whatever the effect of the command was
    # Relies on:	Nothing
    #-
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
    ''' Place-holder for more sophisticated thing; prints '''
    #+
    # Arguments:	string1 (usually the calling procedure's name)
    #			string2 (usually the error message)
    # Returns:		None
    # Side Effects:	prints out the message
    # Relies on:	Nothing
    #-
    print(string1 + '  ' + string2)


####
# Test parse byte-stream of list of dicts back into a list of strings
# each of which can be unpacked later into a dict
def stringtodict(instring):
    ''' Attempt to parse byte-stream list of dicts into a list of strings '''
    #+
    # Arguments:	byte-stream string from REST server or ilk
    # Returns:		list of strings delimited by { and }
    # Side Effects:	None
    # Relies on:	Nothing
    #-
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
    ''' Decode byte-stream returned by web '''
    #+
    # Arguments:	byte-stream string from REST server or ilk
    # Returns:		string decoded with utf-8
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    try:
        relaxed = str(answer.decode("utf-8"))
    except:
        try:
            relaxed = str(answer)
        except:
            relaxed = answer
    return relaxed

def unNone(u_answer):
    ''' Get rid of DB bare "None" and replace w/ quotes '''
    #+
    # Arguments:	string that may have "None" in it
    # Returns:		string with None replaced with quotes so python json doesn't barf
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    try:
        return u_answer.replace(' None ', ' \"None\" ')
    except:
        return u_answer


def globusjson(localuuid, localdir, remotesystem, idealdir): 
    ''' Create a json file for a globus sync job '''
    #+
    # Arguments:	local bundle uuid
    #			local bundle directory
    #			globus name of target system (usually at NERSC)
    #			ideal directory of the bundle
    # Returns:		json that jade-lta can use
    # Side Effects:	None
    # Relies on:	Nothing
    #-
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
    ''' Change the bundle's status to the new one '''
    #+
    # Arguments:	bundle's id
    #			new status (must be valid)
    # Returns:		stdout from operation (or Failure)
    #			stderr from operation (or my message)
    #			errorcode from operation
    # Side Effects:	print if problem
    #			Change in REST server database
    # Relies on:	REST server working
    #-
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
    ''' Return the difference in time between the given time and now '''
    #+
    # Arguments:	old time in '%Y-%m-%d %H:%M:%S' format
    # Returns:		integer time difference in minutes
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    current = datetime.datetime.now()
    try:
        oldt = datetime.datetime.strptime(oldtimestring, '%Y-%m-%d %H:%M:%S')
        difference = current - oldt
        delta = int(difference.seconds/60 + difference.days*60*24)
    except:
        delta = -1
    return delta

###
# Make changes in the BundleStatus specified
# 2-step process:
# Get the number of entries that will be touched
# If zero, return 'None'
# If one, do it and return 'OK'
# IF more than 1,
# If the flag says do them all, do them all, otherwise
# don't do anything at all and return 'TooMany'
def patchBundle(bundleid, columntype, newvalue, manyok):
    ''' Modify info about the given bundle 
         2-step process:  get # of entries that would be touched
         if ==0, return 'None'
         if ==1, do it, return 'OK'
         if >1 && manyok, do them all, return 'OK', else return 'TooMany'
        '''
    #+
    # Arguments:	bundle's id
    #			column name
    #			new value
    #			do many at once?
    # Returns:		'OK' or 'None' or 'TooMany'
    # Side Effects:	print message if problem
    #			crash if there was a problem
    #			possible change in database
    # Relies on:	REST server working
    #-
    #
    geturlx = copy.deepcopy(basicgeturl)
    geturlx.append(targetbundleget + mangle(str(bundleid)))
    ansx, errx, codx = getoutputerrorsimplecommand(geturlx, 1)
    if len(ansx) <= 0:
        print('patchBundle initial query failed failed', ansx, errx, codx, bundleid)
        sys.exit(12)
    try:
        my_jsonx = json.loads(singletodouble(massage(ansx)))
    except:
        print('patchBundle initial query got junk', ansx, bundleid)
        sys.exit(12)
    if len(my_jsonx) == 0:
        return 'None'
    if len(my_jsonx) > 1 and not manyok:
        return 'TooMany'
    #
    posturlx = copy.deepcopy(basicposturl)
    comm = str(bundleid) + ':' + str(columntype)+ ':' + str(newvalue)
    posturlx.append(targetbundlepatch + mangle(comm))
    ansx, errx, codx = getoutputerrorsimplecommand(posturlx, 1)
    if 'OK' not in ansx:
        print('patchBundle update failed', ansx, errx, codx, comm)
        sys.exit(12)
    return 'OK'
#
######################################################
######
# DB connection established
def getdbgen():
    ''' Open connection to mysql database '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	print message if problem
    #			crash if problem
    #			possible change in database
    # Relies on:	mysql database working
    #			~/.my.cnf has valid credentials
    #			global DBdatabase
    #			global DBcursor
    #-
    global DBdatabase
    global DBcursor
    try:
        # https://stackoverflow.com/questions/27203902/cant-connect-to-database-pymysql-using-a-my-cnf-file
        DBdatabase = pymysql.connect(read_default_file='~/.my.cnf',)
    except pymysql.OperationalError:
        print(['ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(11)
    except Exception:
        print(['ERROR: Generic failure to connect to MySQL archivedump database.', sys.exc_info()[0]])
        sys.exit(11)
    try:
        #DBcursor = DBdatabase.cursor()
        DBcursor = DBdatabase.cursor(pymysql.cursors.DictCursor)
    except pymysql.OperationalError:
        print(['ERROR: could not get cursor for database.'])
        sys.exit(11)
    except Exception:
        print(['ERROR: Generic failure to get cursor for database.', sys.exc_info()[0]])
        sys.exit(11)

####
def returndbgen():
    ''' Return cursor to mysql database '''
    #+
    # Arguments:	None
    # Returns:		cursor to database.  Hope it is active!
    # Side Effects:	None
    # Relies on:	global DBcursor
    #-
    global DBcursor
    #
    return DBcursor

####
def closedbgen():
    ''' Disconnect cursor and database connection to mysql database '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	Disconnects from database
    # Relies on:	global DBcursor
    #			global DBdatabase
    #-
    global DBdatabase
    global DBcursor
    #
    DBcursor.close()
    DBdatabase.close()

####
# Commit changes to DB specified
def doCommitDB():
    ''' Commit changes to mysql database.  DumpStream code does not use this '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	print message if problem
    #			crash if problem
    #			possible change in database
    # Relies on:	mysql database working
    #			global DBdatabase
    #-
    global DBdatabase
    #
    try:
        DBdatabase.commit()
    except pymysql.OperationalError:
        DBdatabase.rollback()
        print(['doCommitDB: ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(11)
    except Exception:
        DBdatabase.rollback()
        print(['doCommitDB: Failed to execute the commit'])
        sys.exit(11)



############################################
######  Execute a command.  Crash if it fails, otherwise return silently
def doOperationDB(dbcursor, command, string):
    ''' Execute a mysql command, crash if failure, return nothing '''
    #+
    # Arguments:	active mysql db cursor
    #			mysql command
    #			string to print out if failure
    # Returns:		None
    # Side Effects:	print message if problem
    #			crash if problem
    #			possible change in database
    # Relies on:	mysql database working
    #-
    try:
        dbcursor.execute(command)
        return
    except pymysql.OperationalError:
        print(['ERROR: doOperationDB could not connect to MySQL ', string, ' database.', command])
        sys.exit(11)
    except Exception:
        print(['ERROR: doOperationDB undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(11)
    return

############################################
######  Execute a command.  Crash if it fails, return False if it didn't work right, True if it was OK
def doOperationDBWarn(dbcursor, command, string):
    ''' Execute a mysql command, return success '''
    #+
    # Arguments:	active mysql db cursor
    #			mysql command
    #			string to print out if failure
    # Returns:		boolean for success or failure
    # Side Effects:	print message if problem
    #			crash if connection failure
    #			possible change in database
    # Relies on:	mysql database working
    #-
    try:
        dbcursor.execute(command)
        return True
    except pymysql.OperationalError:
        print(['ERROR: doOperationDBWarn could not connect to MySQL ', string, ' database.', command])
        sys.exit(11)
    except pymysql.IntegrityError:
        print(['ERROR: doOperationDBWarn \"IntegrityError\", probably duplicate key', string, ' database.', sys.exc_info()[0], command])
        return False
    except Exception:
        print(['ERROR: doOperationDBWarn undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(11)
    return True

###
# Globus transfers go by directory, not bundle; at least in our
# sync-up paradigm
def AddActiveDir(a_dirname):
    ''' Add the name to ActiveDirectory '''
    #+
    # Arguments:        ideal directory name 
    # Returns:          ''
    # Side Effects:     print error if problem
    #                   change entry in ActiveDirectory
    # Relies on:        REST server working
    #-
    posturl = copy.deepcopy(basicposturl)
    posturl.append(targetbundleactivediradd + mangle(a_dirname))
    try:
        a_ans, a_err, a_code = getoutputerrorsimplecommand(posturl, 20)
        if len(ans) > 2:
            print('AddActiveDir returned', a_dirname, str(ans))
    except:
        print('AddActiveDir error', a_dirname, str(a_ans), a_err, a_code)
    return ''

def RemoveActiveDir(a_dirname):
    ''' Remove the name from ActiveDirectory '''
    #+
    # Arguments:        ideal directory name or fragment
    # Returns:          ''
    # Side Effects:     print error if problem
    #                   change entry in ActiveDirectory
    # Relies on:        REST server working
    #-
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetbundleactivedirremove + mangle(a_dirname))
    try:
        a_ans, a_err, a_code = getoutputerrorsimplecommand(geturl, 20)
        if len(ans) > 2:
            print('RemoveActiveDir returned', a_dirname, str(a_ans))
    except:
        print('RemoveActiveDir error', a_dirname, str(a_ans), a_err, a_code)
    return ''

def FindActiveDir(a_dirname):
    ''' Look for this name in ActiveDirectory '''
    #+
    # Arguments:        ideal directory name or fragment
    # Returns:          [directory,changetime] if it exists in ActiveDirectory, else []
    # Side Effects:     print error if problem
    # Relies on:        REST server working
    #-
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetbundleactivedirfind + mangle(a_dirname))
    try:
        a_ans, a_err, a_code = getoutputerrorsimplecommand(geturl, 20)
        return singletodouble(massage(a_ans))
    except:
        print('FindActiveDir error', a_dirname, str(a_ans), a_err, a_code)
    return []

###
#
def FindBundlesWithDir(a_dirname, a_status='Unknown'):
    ''' Find bundles from BundleStatus with this directory name and status '''
    #+
    # Arguments:	directory name
    #			status we are looking for (default=Unknown)
    # Returns:		list of arrays of [id, idealName, status] of bundles
    #			 in this directory
    # Side Effects:	print error if problem
    # Relies on:	REST server working
    #-
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetbundlegetlike + mangle(a_dirname + ' ' + str(a_status)))
    try:
        a_ans, a_err, a_code = getoutputerrorsimplecommand(geturl, 20)
    except:
        print('FindBundlesWithDir error', a_dirname, str(a_ans), a_err, a_code)
        return []
    if len(a_ans) < 2:
        return []
    try:
        janswer = json.loads(unNone(singletodouble(massage(a_ans))))
    except:
        print('FindBundlesWithDir: failed to get json info', a_ans)
        return []
    good_stuff = []
    # sanity check--is this the right directory?  Do not go too high!
    for my_json in janswer:
        if a_dirname == os.path.dirname(my_json['idealName']):
            m_id = my_json['bundleStatus_id']
            m_name = my_json['idealName']
            m_status = my_json['status']
            good_stuff.append([m_id, m_name, m_status])
    return good_stuff

def RetrieveDesiredTrees():
    ''' Get the desired trees from the DB '''
    #+
    # Arguments:        None
    # Returns:          list of wanted trees (e.g. ARA/YEAR/unbiased/SPS-NUPHASE
    # Side Effects:     print and die on error
    # Relies on:        REST server working
    #-
    i1geturl = copy.deepcopy(basicgeturl)
    i1geturl.append(targetdumpingwantedtrees)
    i1outp, i1erro, i1code = getoutputerrorsimplecommand(i1geturl, 1)
    if int(i1code) != 0 or 'FAILURE' in str(i1outp):
        print('Get trees failure', i1geturl, i1outp, i1erro)
        sys.exit(0)
    desiredtrees = []
    my_json = json.loads(singletodouble(i1outp))
    for js in my_json:
        h = js['dataTree']
        desiredtrees.append(h)
    return desiredtrees

###
# Return the target directory for copying "to"
def GiveTarget():
    ''' Retrieve the target directory for dumping to '''
    #+
    # Arguments:        None
    # Returns:          target directory
    # Side Effects:     print and die on failure
    # Relies on:        REST server working
    #-
    gtgeturl = copy.deepcopy(basicgeturl)
    gtgeturl.append(targetdumpingdumptarget)
    gtoutp, gterro, gtcode = getoutputerrorsimplecommand(gtgeturl, 1)
    if int(gtcode) != 0 or 'FAILURE' in str(gtoutp):
        print('Get Target directory failed', gtoutp, gterro)
        sys.exit(0)
    dump_json = json.loads(singletodouble(gtoutp))
    try:
        directory = dump_json[0]['target']
    except:
        print('Cannot unpack', dump_json)
        sys.exit(0)
    return directory


############################################
######  Execute a command, return the answer.  Or error messages if it failed
def doOperationDBTuple(dbcursor, command, string):
    ''' Execute a database operation on a mysql database w/ dbcursor established
        The command is command
        string is info '''
    #+
    # Arguments:	dbcursor:  cursor established for mysql database
    #			command:  mysql command to execute
    #			string:	info to print in case of error
    # Returns:		tuple, contents depend on the command
    #			list with error info
    # Side Effects:	mysql DB may change; depends on command
    # Relies on:	mysql DB available, connection works
    #			pymysql
    #-
    try:
        dbcursor.execute(command)
        expectedtuple = dbcursor.fetchall()
        return expectedtuple		#Assume you know what you want to do with this
    except pymysql.OperationalError:
        return ['ERROR: doOperationDBTuple could not connect to MySQL ', string, ' database.', command]
    except Exception:
        return ['ERROR: doOperationDBTuple undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command]
    return [[]]

####
# Utility for paring off the jade file key prefix to a filename
def NormalName(filename):
    ''' Return the base file name without any ukey prefix.  May be expanded
        with new prefixes to remove as needed '''
    #+
    # Arguments:        file name
    # Returns:          base file name without cruft
    # Side Effects:     None
    # Relies on:        Nothing
    #-
    # Assumption:  some files have ukey_${UUID}_rest-of-the-file-name
    #   we want 'rest-of-the-file-name'
    localstr = str(filename)
    directorypart = os.path.dirname(localstr)
    basepart = os.path.basename(localstr)
    chunks = basepart.split('_')
    nch = len(chunks)
    if chunks[0] != 'ukey':
        return localstr
    nna = ''
    for i in range(2, nch-1):
        nna += chunks[i] + '_'
    nna += chunks[nch-1]
    return directorypart + '/' + nna

####
# Utility for determining if a file is in the generic
# tree (with "YEAR" as placeholder) 
def TreeComp(tctree, tcfile):
    ''' Compare a desired tree (e.g. IceCube/YEAR/internal-system/pDAQ-2ndBld)
        with a found file.  If it matches, return True '''
    #-
    # Arguments:    tree format (e.g. IceCube/YEAR/internal-system/pDAQ-2ndBld)
    #               file name (e.g. /mnt/slot6/IceCube/2018/internal-system\
    #                /pDAQ-2ndBld/1208/ukey_fffc0e97-c984-4faa-8d9c-ed60f2ccfe46\
    #                _SPS-pDAQ-2ndBld-000_20181208_211302_000000.tar.gz
    # Returns:      Boolean if the tree format matches the file name
    # Side Effects: None
    # Relies on:    Nothing
    #-
    chunks = tctree.split('YEAR')
    firstindex = tcfile.find(chunks[0])
    if firstindex < 0:
        return False
    secondindex = tcfile.find(chunks[1], firstindex)
    if secondindex < 0:
        return False
    return True
