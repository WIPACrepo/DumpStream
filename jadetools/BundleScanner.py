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
        return "", error, 1

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
######################################################
######
# DB connection established
def getdbgen():
    try:
        global DBdatabase
        global DBcursor
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


########################################################
# Define Phases for Main

# Phase 0
# Look for space usage
# Log the space usage and date
# Done w/ phase 0
def Phase0():
    #storageArea = '/var/log'
    storageArea = '/mnt/lfss'
    command = ['/bin/df', '-BG', storageArea]
    ErrorString = ''
    outp, erro, code = getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        ErrorString = ErrorString + ' Failed to df '
    else:
        lines = outp.decode("utf-8").splitlines()
        size = -1
        for line in lines:
            if storageArea in str(line):
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
    danswer = massage(answer)
    if 'OK' not in danswer:
        print(answer)
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle('Failed to set poolsize'))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        return
    return

#targetsetdumperror = curltargethost + '/dumpcontrol/update/bundleerror/'
#targetsetdumpstatus = curltargethost + '/dumpcontrol/update/status/'
#targetsetdumppoolsize = curltargethost + '/dumpcontrol/update/poolsize/'
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
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle(' Failed to ls ' + GLOBUS_PROBLEM_SPACE))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        return
    # If here, ls worked ok.
    lines = outp.decode("utf-8").splitlines()
    if len(lines) == 0:
        return	# OK, nothing to do
    for line in lines:
        if '.json' in str(line):
            words = str(line).split('.json')
            filefragment = words[0]
            geturl = copy.deepcopy(basicgeturl)
            trysql = 'SELECT bundleStatus_id,idealName,status FROM BundleStatus WHERE'
            trysql = trysql + ' UUIDJade = \"' + filefragment + '\" AND status=\"JsonMade\"'
            #print(trysql)
            geturl.append(targetupdatebundle + mangle(trysql))
            answer = getoutputsimplecommandtimeout(geturl, 1)
            #danswer = answer.decode("utf-8")
            danswer = massage(answer)
            if danswer == '':
                continue
            if 'FAILURE' in str(danswer):
                ErrorString = ErrorString + ' FAILURE WITH ' + str(filefragment)
                break
            #
            # What happens if I get multiple returns?????  DEBUG
            #print(type(danswer),danswer)
            janswer = json.loads(singletodouble(danswer))
            if len(janswer) <= 0:
                ErrorString = ErrorString + ' No DB info for ' + str(filefragment)
                break
            if len(janswer) > 1:
                ErrorString = ErrorString + ' Multiple active versions of ' + str(filefragment)
                break
            #print(type(janswer),janswer)
            #print(type(janswer[0]),janswer[0])
            bsid = janswer[0]['bundleStatus_id']
            posturl = copy.deepcopy(basicposturl)
            trysql = 'UPDATE BundleStatus SET status=\"PushProblem\" WHERE BundleStatus_id=' + str(bsid)
            #print(trysql)
            posturl.append(targetupdatebundle + mangle(trysql))
            answer = getoutputsimplecommandtimeout(posturl, 1)
            #print(answer)
            command = ['/bin/mv', GLOBUS_PROBLEM_SPACE + '/' + str(line), GLOBUS_PROBLEM_HOLDING]
            outp, erro, code = getoutputerrorsimplecommand(command, 1)
            if int(code) != 0:
                ErrorString = ErrorString + ' Failed to move ' + str(line)

    if ErrorString != '':
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle(ErrorString))
        answer = getoutputsimplecommandtimeout(posturl, 1)
    #
    # I have not implemented the rm of the Alert file
    return

# Phase 2	Look for transferred files
# ls GLOBUS_DONE_SPACE
# When you find some, move them to GLOBUS_DONE_HOLDING and
# update their DB entries to PushDone
def Phase2():
    command = ['/bin/ls', GLOBUS_DONE_SPACE]
    ErrorString = ''
    outp, erro, code = getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle(' Failed to ls ' + GLOBUS_DONE_SPACE))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        return
    # If here, ls worked ok.
    lines = outp.decode("utf-8").splitlines()
    if len(lines) == 0:
        return	# OK, nothing to do
    for line in lines:
        if '.json' not in str(line):
            continue
        words = str(line).split('.json')
        filefragment = words[0]
        geturl = copy.deepcopy(basicgeturl)
        trysql = 'SELECT bundleStatus_id,idealName,status FROM BundleStatus WHERE'
        trysql = trysql + ' UUIDJade = \"' + filefragment + '\" AND status=\"JsonMade\"'
        #print(trysql)
        geturl.append(targetupdatebundle + mangle(trysql))
        answer = getoutputsimplecommandtimeout(geturl, 1)
        #danswer = answer.decode("utf-8")
        danswer = massage(answer)
        if danswer == '':
            continue
        if 'FAILURE' in str(danswer):
            ErrorString = ErrorString + ' FAILURE WITH ' + str(filefragment)
            break
        #
        # What happens if I get multiple returns?????  DEBUG
        #print(type(danswer),danswer)
        janswer = json.loads(singletodouble(danswer))
        if len(janswer) <= 0:
            ErrorString = ErrorString + ' No DB info for ' + str(filefragment)
            break
        if len(janswer) > 1:
            ErrorString = ErrorString + ' Multiple active versions of ' + str(filefragment)
            break
        #print(type(janswer),janswer)
        #print(type(janswer[0]),janswer[0])
        bsid = janswer[0]['bundleStatus_id']
        posturl = copy.deepcopy(basicposturl)
        trysql = 'UPDATE BundleStatus SET status=\"PushDone\" WHERE BundleStatus_id=' + str(bsid)
        #print(trysql)
        posturl.append(targetupdatebundle + mangle(trysql))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        #print(answer)
        command = ['/bin/mv', GLOBUS_DONE_SPACE + '/' + str(line), GLOBUS_DONE_HOLDING]
        outp, erro, code = getoutputerrorsimplecommand(command, 1)
        if int(code) != 0:
            ErrorString = ErrorString + ' Failed to move ' + str(line)

    if ErrorString != '':
        posturl = copy.deepcopy(basicposturl)
        posturl.append(targetsetdumperror + mangle(ErrorString))
        answer = getoutputsimplecommandtimeout(posturl, 1)
    #
    return


# Phase 3	Look for new local files
# Get list of local bundle tree locations relevant to NERSC transfers
def Phase3():
    geturl = copy.deepcopy(basicgeturl)
    ultimate = '\"NERSC\"'
    geturl.append(targettree + mangle(ultimate))
    answer = getoutputsimplecommandtimeout(geturl, 1)
    danswer = massage(answer)
    ErrorString = ''
    candidateList = []
    if danswer == '':
        print('No place to search')
        return		# Dunno, maybe this was deliberate
    if 'FAILURE' in danswer:
        print(danswer)
        return
    #print(danswer)
    jdiranswer = json.loads(singletodouble(danswer))
    #print(len(jdiranswer), janswer)
    for js in jdiranswer:
        dirs = js['treetop']
        command = ['/bin/find', dirs, '-type', 'f']
        outp, erro, code = getoutputerrorsimplecommand(command, 30)
        if int(code) != 0:
            print(' Failed to find/search ' + str(dirs))
            ErrorString = ErrorString + ' Failed to find/search ' + str(dirs)
        if len(outp) == 0:
            continue
        #print(outp)
        lines = outp.splitlines()
        for t in lines:
            if '.zip' in str(t):
                candidateList.append(t.decode("utf-8"))
    if len(candidateList) <= 0:
        return
    # OK, found a curious limit with sqlite3.  I cannot use
    #  more than 25 string entries in the localName IN () query
    # So, I have to break it up into multiple queries
    inchunkCount = 0
    jsonList = []
    nomatch = []
    for p in candidateList:
        if inchunkCount == 0:
            bigquery = 'status!=\"Abort\" AND localName IN ('
        bigquery = bigquery + '\"' + p + '\",'
        inchunkCount = inchunkCount + 1
        if inchunkCount > 24:	# Avoid count limit
            inchunkCount = 0
            # replace the last comma with a right parenthesis
            bigq = bigquery[::-1].replace(',', ')', 1)[::-1]
            #print('bigq=', bigq)
            geturl = copy.deepcopy(basicgeturl)
            geturl.append(targetfindbundles + mangle(bigq))
            answer = getoutputsimplecommandtimeout(geturl, 3)
            danswer = massage(answer)
            if len(danswer) < 1:
                continue
            if 'Not Found' in danswer:
                print('Not Found', danswer)
                #print(bigq)
                continue
            else:
                try:
                    jjanswer = json.loads(singletodouble(danswer))
                except:
                    print('Failed to translate json code', danswer)
                    return
                for js in jjanswer:
                    jsonList.append(js)
        #
    if inchunkCount > 0:
        bigq = bigquery[::-1].replace(',', ')', 1)[::-1]
        #print('bigq=', bigq)
        geturl = copy.deepcopy(basicgeturl)
        geturl.append(targetfindbundles + mangle(bigq))
        answer = getoutputsimplecommandtimeout(geturl, 3)
        danswer = massage(answer)
        #print('danswer length=', len(danswer))
        if len(danswer) > 1:
            if 'Not Found' in danswer:
                print('Not Found', danswer)
                #print('bigq=', bigq)
                return
            else:
                try:
                    jjanswer = json.loads(singletodouble(danswer))
                except:
                    print('Failed to translate json code', danswer)
                    return
                for js in jjanswer:
                    jsonList.append(js)

    if len(jsonList) == 0:
        for p in candidateList:
            nomatch.append(p)
    else:
        for p in candidateList:
            mfound = False
            for js in jsonList:
                if p == js['localName']:
                    mfound = True
                    break
            if not mfound:
                nomatch.append(p)
    if len(nomatch) == 0:
        #print(len(nomatch), 'No matches found')
        return		# All present and accounted for
    #
    # OK, now check that the info is in the database.  Connect to
    # jade-lta-db now.  Use the .my.cnf so I don't expose passwords
    getdbgen()
    cursor = returndbgen()
    #
    # It seems reasonabl to think that a particular file will only
    # exist once in the jade-lta-db.  Unique UUID and all..
    for filex in nomatch:
        mybasename = os.path.basename(filex)
        reply = doOperationDBTuple(cursor, 'SELECT * FROM jade_bundle WHERE bundle_file=\"' + mybasename + '\"', 'Phase3')
        if 'ERROR' in reply:
            continue
        #print(reply[0]['size'], reply[0]['checksum'])
        #
        for js in jdiranswer:
            dirs = js['treetop']
            nc = filex.split(dirs)
            if len(nc) == 2:
                insdict = '\{\'localName\' : \'' + filex+ '\', \'idealName\' : \'' + nc[1] + '\', \'size\' : \'' + str(reply[0]['size'])
                insdict = insdict + '\', \'checksum\' : \'' + str(reply[0]['checksum']) + '\', \'UUIDJade\' : \'\', \'UUIDGlobus\' : \'\','
                insdict = insdict + ' \'useCount\' : \'1\', \'status\' : \'Untouched\'\}'
                #print(mangle(str(insdict)))
                posturl = copy.deepcopy(basicposturl)
                posturl.append(targetaddbundle + mangle(str(insdict)))
                answer = getoutputsimplecommandtimeout(posturl, 1)
                if 'OK' not in str(answer):
                    print(str(insdict), answer)
                continue
    return
# Foreach tree
#   Find bundle files in each tree
#   Accumulate information in locallist
# If locallist is not empty
#   Build a query for contents of locallist
# Query BundleStatus for these
# Parse the return:
# If the file is not in BundleStatus, add it as 'Untouched'
# Done with phase 3


###########
# Phase 4	Submit new files
def Phase4():
    geturl = copy.deepcopy(basicgeturl)
    geturl.append(targetdumpinfo)
    answer = massage(getoutputsimplecommandtimeout(geturl, 1))
    janswer = json.loads(singletodouble(answer))
    # I know a priori there is only one return line
    status = janswer['status']
    if status != 'Run':
        #print('No run')
        return		# Don't load more in the globus pipeline
    geturl = copy.deepcopy(basicgeturl)
    #geturl.append(targetuntouchedall)
    geturl.append(targetuntouchedall)
    #print(geturl)
    answer = massage(getoutputsimplecommandtimeout(geturl, 1))
    #print(answer)
    if 'DOCTYPE HTML PUBLIC' in answer or 'FAILURE' in answer:
        print('Error in answer')
        return
    # There may be multiple entries here
    #print(answer)
    jjanswer = json.loads(singletodouble(answer))
    numwaiting = len(jjanswer)
    #print(numwaiting)
    if numwaiting <= 0:
        #print('None waiting')
        return		# Nothing to do
    command = ['/bin/ls', GLOBUS_RUN_SPACE]
    answerlsb, errorls, codels = getoutputerrorsimplecommand(command, 1)
    #print('code=', str(codels))
    if int(codels) != 0:
        print('Cannot ls the', GLOBUS_RUN_SPACE)
        return		# Something went wrong, try later
    answerls = massage(answerlsb)
    if 'TIMEOUT' in answerls:
        print('ls timed out')
        return		# Something went wrong, try later
    lines = answerls.splitlines()
    if len(lines) >= GLOBUS_INFLIGHT_LIMIT:
        #print('Too busy')
        return		# Too busy for more
    limit = GLOBUS_INFLIGHT_LIMIT - len(lines)
    if limit > numwaiting:
        limit = numwaiting
    #print('limit=', str(limit), str(len(lines)), str(numwaiting))
    for countup in range(0, limit):
        try:
            js = jjanswer[countup]
            bundle_id = js['bundleStatus_id']
            localName = js['localName']
            idealName = js['idealName']
        except:
            print('Failure in unpacking json info for #', str(countup))
            return
        jadeuuid = str(uuid.uuid4())
        localDir = os.path.dirname(localName) + '/'
        idealDir = os.path.dirname(idealName) + '/'
        remotesystem = 'NERSC'
        jsonContents = globusjson(jadeuuid, localDir, remotesystem, idealDir)
        jsonName = jadeuuid + '.json'
        try:
            fileout = open(GLOBUS_RUN_SPACE + '/' + jsonName, 'w')
            fileout.write(jsonContents)
            fileout.close()
        except:
            print('Failed to open/write/close ' + jsonName)
            return	# Try again later
        # Now update the BundleStatus
        posturl = copy.deepcopy(basicposturl)
        trysql = 'UPDATE BundleStatus SET status=\"JsonMade\",UUIDJade=\"{}\" WHERE BundleStatus_id='.format(jadeuuid) + str(bundle_id)
        #print(trysql)
        posturl.append(targetupdatebundle + mangle(trysql))
        answer = getoutputsimplecommandtimeout(posturl, 1)
        # Not checking answer is probably a bad thing hereJNB
        continue
    return
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
#   update the BundleStatus for this bundle to 'JsonMade' 
Phase0()
Phase1()
Phase2()
Phase3()
Phase4()
