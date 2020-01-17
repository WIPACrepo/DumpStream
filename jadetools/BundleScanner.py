# BundleScanner.py (.base)
import sys
import uuid
import json
import copy
import os
import Utils as U

DEBUGIT = False

##################
# Utilities
###
# Create a new localname
def backcompare(local, ideal):
    # find how much these agree, working from the right
    # Then create a new directory using the residue of the
    # local name and the ideal name
    localwords = local.split('/')
    idealwords = ideal.split('/')
    danswer = ''
    if localwords[-1] != idealwords[-1]:
        return danswer
    # Also check if the match is exact
    perfect = 0
    for i in range(2, min(len(localwords), len(idealwords))):
        if localwords[-i] == idealwords[-i]:
            continue
        q = ''
        for word in localwords[0:-i+1]:
            q = q + '/' + word
        danswer = (q+ideal).replace('//', '/')
        perfect = 1
        break
    if perfect == 0:
        return local	# perfect match
    return danswer	# constructed match

###
# Quick utility for finding DumpCandC status
def FindDumpCandC():
    ''' Get control info for Bundle dump control '''
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetdumpinfo)
    answer1, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    # Sanity check needed here.
    answer = U.massage(answer1)
    janswer = json.loads(U.singletodouble(answer))
    # I know a priori there can be only one return line
    status = janswer['status']
    return status

###
# Check all bundles w/ the same directory and Unknown status
#  and update them to JsonMade
def UpdateBundlesWithDirToJsonMade(a_dirname):
    ''' Update all bundles w/ a_dirname and Unknown to JsonMade '''
    info_array = U.FindBundlesWithDir(a_dirname, 'Unknown')
    if len(info_array) <= 0:
        return ''
    for row in info_array:
        bid = row[0]
        l_ok = U.patchBundle(bid, 'status', 'JsonMade', False)
        if l_ok != 'OK':
            print('UpdateBundlesWithDirToJsonMade:', row, l_ok)
    return ''

###
#
def CleanActiveDir():
    ''' Ask for ActiveDirectory rows with no active transfers to be deleted '''
    # Most of the work happens in the REST server--less to and fro that way
    posturl = copy.deepcopy(U.basicposturl)
    posturl.append(U.targetbundleactivedirclean)
    answer1, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
    if 'FAILURE' in str(answer1):
        print('CleanActiveDir got a failure:', answer1, erro, code)
    return ''

###
# Check for the number of running jobs and determine how many
# we can submit
def CheckRunningGlobus() -> int:
    ''' How many globus jobs can I run? '''
    command = ['/bin/ls', U.GLOBUS_RUN_SPACE]
    try:
        answerlsb, errorls, codels = U.getoutputerrorsimplecommand(command, 1)
    except:
        print('CheckRunningGlobus failure', answerlsb, errorls, codels)
        return 0
    #
    if int(codels) != 0:
        print('Cannot ls the', U.GLOBUS_RUN_SPACE, errorls)
        return 0        # Something went wrong, try later
    answerls = U.massage(answerlsb)
    if 'TIMEOUT' in answerls:
        print('ls timed out')
        return 0        # Something went wrong, try later
    lines = answerls.splitlines()
    if len(lines) >= U.GLOBUS_INFLIGHT_LIMIT:
        #print('Too busy')
        return 0        # Too busy for more
    limit = U.GLOBUS_INFLIGHT_LIMIT - len(lines)
    if limit > numwaiting:
        limit = numwaiting
    return limit

###
# Check if the localname tree matches the ideal name tree
# If not, make it so.
def movelocal(local, ideal, bid):
    # The BundleStatus with bundleStatus_id = bid has the
    # localName local and idealName ideal
    # If the file is in the correct place (having a tree
    # like that of the idealName, but sited differently)
    # then just return the local name and do nothing else.
    # Otherwise:
    # get a newlocal name
    # mkdir -p the proper tree
    # mv local newlocal
    # Update the BundleStatus localName
    newlocal = backcompare(local, ideal)
    if newlocal == local:
        return local
    #
    # We've got work to do.
    localdir = os.path.dirname(newlocal)
    command = ['/bin/mkdir', '-p', localdir]
    ansx, errx, codx = U.getoutputerrorsimplecommand(command, 1)
    if codx != 0:
        print('Failure to create new directory', localdir)
        sys.exit(0)
    #
    command = ['/bin/mv', local, newlocal]
    ansx, errx, codx = U.getoutputerrorsimplecommand(command, 1)
    if codx != 0:
        print('Failure to move', local, 'to', newlocal, errx, codx)
        sys.exit(0)
    #
    ansx = U.patchBundle(bid, 'localName', newlocal, False)
    if 'OK' not in ansx:
        print('movelocal DB update failed', ansx)
        sys.exit(0)
    #
    return newlocal

########################################################
# Define Phases for Main

# Phase 0
# Look for space usage
# Log the space usage and date
# Done w/ phase 0
def Phase0():
    ''' Check space usage, is NERSC running, should we run? '''
    #storageArea = '/var/log'
    storageArea = '/mnt/lfss'
    command = ['/bin/df', '-BG', storageArea]
    ErrorString = ''
    outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        ErrorString = ErrorString + ' Failed to df '
    else:
        lines = outp.splitlines()
        size = -1
        for line in lines:
            if storageArea in str(line):
                words = line.split()
                try:
                    sizeword = words[len(words)-3]
                    size = int(sizeword[0:-1])  # remove trailing G
                except:
                    ErrorString = ErrorString + ' Failed to df '
    #
    # Check NERSC: is the client running there?  If not, we've no idea
    # how big the pool size is there and we should stop
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetnerscinfo)
    outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    if int(code) != 0:
        ErrorString = ErrorString + ' Failed to get NERSC info'
    else:
        try:
            my_json = json.loads(U.singletodouble(str(outp)))
            lastscan = U.deltaT(str(my_json['lastChangeTime']))
            if lastscan > U.WAITFORNERSCAFTER:
                ErrorString = ErrorString + ' No NERSC activity'
            if str(my_json['status']) != 'Run':
                ErrorString = ErrorString + ' NERSC not running'
        except:
            ErrorString = ErrorString + ' Failed to unpack NERSC info'
    #
    # Check for errors
    if len(ErrorString) > 0:
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetsetdumperror + U.mangle(ErrorString))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
        return
    posturl = copy.deepcopy(U.basicposturl)
    posturl.append(U.targetsetdumppoolsize + str(size))
    answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
    danswer = U.massage(answer)
    if 'OK' not in danswer:
        print(answer)
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetsetdumperror + U.mangle('Failed to set poolsize'))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
        return

#U.targetsetdumperror = curlU.targethost + '/dumpcontrol/update/bundleerror/'
#U.targetsetdumpstatus = curlU.targethost + '/dumpcontrol/update/status/'
#U.targetsetdumppoolsize = curlU.targethost + '/dumpcontrol/update/poolsize/'
# 
# Phase 1	Look for problem files
# ls /mnt/data/jade/problem_files/globus-mirror
# If U.GLOBUS_PROBLEM_SPACE
# If alert file is only file present, delete it and we're done
# foreach .json file in the list
#    Update BundleStatus for each with 'PushProblem'
# Update CandC with status='Error', bundleError='problem_files'
# Done with phase 1
def Phase1():
    ''' Phase1 look for problem files '''
    command = ['/bin/ls', U.GLOBUS_PROBLEM_SPACE]
    ErrorString = ''
    outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetsetdumperror + U.mangle(' Failed to ls ' + U.GLOBUS_PROBLEM_SPACE))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
        return
    # If here, ls worked ok.
    lines = outp.splitlines()
    if len(lines) == 0:
        return	# OK, nothing to do
    for line in lines:
        if '.json' in str(line):
            words = str(line).split('.json')
            filefragment = words[0]
            geturl = copy.deepcopy(U.basicgeturl)
            geturl.append(U.targetbundleinfobyjade + U.mangle(filefragment + ' JsonMade'))
            answer, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
            danswer = U.massage(answer)
            if danswer == '':
                continue
            if 'FAILURE' in str(danswer):
                ErrorString = ErrorString + ' FAILURE WITH ' + str(filefragment)
                break
            #
            # What happens if I get multiple returns?????  DEBUG
            janswer = json.loads(U.singletodouble(danswer))
            if len(janswer) <= 0:
                continue   # Not relevant to our activity
                #ErrorString = ErrorString + ' No DB info for ' + str(filefragment) + ' as JsonMade'
            if len(janswer) > 1:
                ErrorString = ErrorString + ' Multiple active versions of ' + str(filefragment)
                break
            bsid = janswer[0]['bundleStatus_id']
            answer, erro, code = U.flagBundleStatus(str(bsid), 'PushProblem')
            command = ['/bin/mv', U.GLOBUS_PROBLEM_SPACE + '/' + str(line), U.GLOBUS_PROBLEM_HOLDING]
            outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
            if int(code) != 0:
                ErrorString = ErrorString + ' Failed to move ' + str(line)

    if ErrorString != '':
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetsetdumperror + U.mangle(ErrorString))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
    #
    # I have not implemented the rm of the Alert file
    return

# Phase 2	Look for transferred files
# ls U.GLOBUS_DONE_SPACE
# When you find some, move them to U.GLOBUS_DONE_HOLDING and
# update their DB entries to PushDone
def Phase2():
    ''' Phase2 Look in transfer-done area for transferred files '''
    command = ['/bin/ls', U.GLOBUS_DONE_SPACE]
    ErrorString = ''
    outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
    if int(code) != 0:
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetsetdumperror + U.mangle(' Failed to ls ' + U.GLOBUS_DONE_SPACE))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
        return
    # If here, ls worked ok.
    lines = outp.splitlines()
    if len(lines) == 0:
        return	# OK, nothing to do
    for line in lines:
        if '.json' not in str(line):
            continue
        words = str(line).split('.json')
        filefragment = words[0]
        geturl = copy.deepcopy(U.basicgeturl)
        geturl.append(U.targetbundleinfobyjade + U.mangle(filefragment + ' JsonMade'))
        answer, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
        danswer = U.massage(answer)
        if danswer == '':
            continue
        if 'FAILURE' in str(danswer):
            ErrorString = ErrorString + ' FAILURE WITH ' + str(filefragment)
            break
        #
        # What happens if I get multiple returns?????  DEBUG
        #print(type(danswer),danswer)
        janswer = json.loads(U.singletodouble(danswer))
        if len(janswer) <= 0:
            continue   # Not relevant to our activity
            #ErrorString = ErrorString + ' No DB info for ' + str(filefragment) + ' as JsonMade'
        if len(janswer) > 1:
            ErrorString = ErrorString + ' Multiple active versions of ' + str(filefragment)
            break
        bsid = janswer[0]['bundleStatus_id']
        answer, erro, code = U.flagBundleStatus(str(bsid), 'PushDone')
        command = ['/bin/mv', U.GLOBUS_DONE_SPACE + '/' + str(line), U.GLOBUS_DONE_HOLDING]
        outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
        if int(code) != 0:
            ErrorString = ErrorString + ' Failed to move ' + str(line)

    if ErrorString != '':
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetsetdumperror + U.mangle(ErrorString))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
    #
    return


# Phase 3	Look for new local files
# Get list of local bundle tree locations relevant to NERSC transfers
def Phase3():
    ''' Phase3 Look for new local bundles '''
    geturl = copy.deepcopy(U.basicgeturl)
    ultimate = 'NERSC'
    geturl.append(U.targettree + U.mangle(ultimate))
    answer, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    danswer = U.massage(answer)
    ErrorString = ''
    candidateList = []
    if danswer == '':
        print('No place to search')
        return		# Dunno, maybe this was deliberate
    if 'FAILURE' in danswer:
        print(danswer, erro, code)
        return
    jdiranswer = json.loads(U.singletodouble(danswer))
    #
    # jdiranswer is the list of directories to search for zip bundles
    #
    for js in jdiranswer:
        dirs = js['treetop']
        command = ['/bin/find', dirs, '-type', 'f']
        outp, erro, code = U.getoutputerrorsimplecommand(command, 30)
        if int(code) != 0:
            print(' Failed to find/search ' + str(dirs))
            ErrorString = ErrorString + ' Failed to find/search ' + str(dirs)
        if len(outp) == 0:
            continue
        #print(outp)
        lines = outp.splitlines()
        for t in lines:
            if '.zip' in str(t):
                candidateList.append(t)
    if len(candidateList) <= 0:
        return
    #
    # candidateList is the list of zip files found.  Presumably these
    # are bundles
    #
    # OK, found a curious limit with sqlite3.  I cannot use
    #  more than 25 string entries in the localName IN () query
    # So, I have to break it up into multiple queries
    inchunkCount = 0
    jsonList = []
    nomatch = []
    for p in candidateList:
        # p is the real localName of the file.  Search for this
        # in the database--maybe we've dealt with it already
        #
        if inchunkCount == 0:
            bigquery = ''
        bigquery = bigquery + p + ','
        inchunkCount = inchunkCount + 1
        if inchunkCount > 15:	# Avoid count limit
            inchunkCount = 0
            # replace the last comma with a right parenthesis
            bigq = bigquery[::-1].replace(',', '', 1)[::-1]
            geturl = copy.deepcopy(U.basicgeturl)
            geturl.append(U.targetfindbundlesin + U.mangle(bigq))
            answer, erro, code = U.getoutputerrorsimplecommand(geturl, 3)
            danswer = U.massage(answer)
            if len(danswer) < 1:
                continue
            if 'Not Found' in danswer:
                print('Not Found', danswer)
                continue
            try:
                jjanswer = json.loads(U.singletodouble(danswer))
            except:
                print('Failed to translate json code A', danswer, geturl)
                return
            for js in jjanswer:
                jsonList.append(js)
        #
    if inchunkCount > 0:
        bigq = bigquery[::-1].replace(',', '', 1)[::-1]
        geturl = copy.deepcopy(U.basicgeturl)
        geturl.append(U.targetfindbundlesin + U.mangle(bigq))
        answer, erro, code = U.getoutputerrorsimplecommand(geturl, 3)
        danswer = U.massage(answer)
        if len(danswer) > 1:
            if 'Not Found' in danswer:
                print('Not Found', danswer)
                return
            try:
                jjanswer = json.loads(U.singletodouble(danswer))
            except:
                print('Failed to translate json code B', danswer, geturl)
                return
            for js in jjanswer:
                jsonList.append(js)
    #
    # Now we have a list of already known bundles, whose info is now
    #  in jsonList
    #
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
        return		# All present and accounted for
    #
    # OK, now check that the info is in the database.  Connect to
    # jade-lta-db now.  Use the .my.cnf so I don't expose passwords
    U.getdbgen()
    cursor = U.returndbgen()
    #
    # It seems reasonable to think that a particular file will only
    # exist once in the jade-lta-db.  Unique UUID and all..
    for filex in nomatch:
        mybasename = os.path.basename(filex)
        reply = U.doOperationDBTuple(cursor, 'SELECT * FROM jade_bundle WHERE bundle_file=\"' + mybasename + '\"', 'Phase3')
        if 'ERROR' in reply:
            continue
        try:
            myjs = reply[0]
            size = str(myjs['size'])
        except:
            print('Phase3:  myjs did not unpack', reply, mybasename)
            continue
            #sys.exit(0)
        idealName = str(myjs['destination']) + '/' + mybasename
        checksum = str(myjs['checksum'])
        insdict = '\{\'localName\' : \'' + filex+ '\', \'idealName\' : \'' + idealName + '\', \'size\' : \'' + size + '\','
        insdict = insdict + ' \'checksum\' : \'' + checksum + '\', \'UUIDJade\' : \'\', \'UUIDGlobus\' : \'\','
        insdict = insdict + ' \'useCount\' : \'1\', \'status\' : \'Untouched\'\}'
        posturl = copy.deepcopy(U.basicposturl)
        posturl.append(U.targetaddbundle + U.mangle(str(insdict)))
        answer, erro, code = U.getoutputerrorsimplecommand(posturl, 1)
        if 'OK' not in str(answer):
            print(str(insdict), answer)
        continue
    #
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
    ''' Phase4 submit new files '''
    # Should I be running?
    status = FindDumpCandC()
    if status != 'Run':
        return		# Don't load more in the globus pipeline
    # Do a little cleanup
    CleanActiveDir()
    #
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetuntouchedall)
    answer1, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    answer = U.singletodouble(U.massage(answer1))
    if len(answer) <= 0:
        return	# silence
    if 'DOCTYPE HTML PUBLIC' in answer or 'FAILURE' in answer:
        print('Error in answer')
        return
    # There may be multiple entries here
    try:
        jjanswer = json.loads(answer)
    except:
        print('Phase4 getting untouched failed', answer)
        sys.exit(0)
    numwaiting = len(jjanswer)
    #print(numwaiting)
    if numwaiting <= 0:
        #print('None waiting')
        return		# Nothing to do
    #
    # Given the number of globus sync jobs nominally running, how many can I do?
    limit = CheckRunningGlobus()
    if limit <= 0:
        return		# Cannot do anything
    #
    for countup in range(0, limit):
        try:
            js = jjanswer[countup]
            bundle_id = js['bundleStatus_id']
            localName = js['localName']
            idealName = js['idealName']
            idealDirA = os.path.dirname(idealName)  # Doesn't have a '/' at end
        except:
            print('Failure in unpacking json info for #', str(countup))
            return
        found_dir_list = U.FindActiveDir(idealDirA)
        if len(found_dir_list) > 2:
            continue	# We are already syncing this directory.  Unless
            # there's been a race, in which case we'll pick it up when the
            # current set is done
        jadeuuid = str(uuid.uuid4())
        newlocal = movelocal(localName, idealName, bundle_id)
        localDir = os.path.dirname(newlocal) + '/'
        idealDir = os.path.dirname(idealName) + '/'
        remotesystem = 'NERSC'
        jsonContents = U.globusjson(jadeuuid, localDir, remotesystem, idealDir)
        jsonName = jadeuuid + '.json'
        try:
            fileout = open(U.GLOBUS_RUN_SPACE + '/' + jsonName, 'w')
            fileout.write(jsonContents)
            fileout.close()
        except:
            print('Failed to open/write/close ' + jsonName)
            return	# Try again later
        #
        # Now update the BundleStatus
        panswer = U.patchBundle(str(bundle_id), 'status', 'JsonMade', False)
        if 'FAILURE' in panswer:
            print('Phase4: bundle update failed', panswer, bundle_id)
            continue
        panswer = U.patchBundle(str(bundle_id), 'UUIDJade', jadeuuid, False)
        if 'FAILURE' in panswer:
            print('Phase4: bundle update failed uuidjade', panswer, bundle_id)
            continue
        #
        # Flag the other bundles in this directory as having JsonMade, since
        # they will also be sync-ed along with this one
        # Bundles added later won't be
        also_list = U.FindBundlesWithDir(idealDirA, 'Unknown')
        if len(also_list) > 0:
            UpdateBundlesWithDirToJsonMade(idealDirA)
        continue
    return
# Check CandC for go/nogo
# If Error or Halt or Drain, done w/ phase 4
# Query BundleStatus for the bundlelist of 'Untouched' bundles,
# starting with the oldest
# If == 0, done w/ phase 4
# Query BundleStatus for the count of JsonMade bundles
# If > U.GLOBUS_INFLIGHT_LIMIT, done w/ phase 4
# foreach bundle in the bundlelist
#   if the running count > U.GLOBUS_INFLIGHT_LIMIT, done w/ phase 4
#   Create a .json file for this bundle in U.GLOBUS_RUN_SPACE
#   update the BundleStatus for this bundle to 'JsonMade' 
#
# Check CandC for Run or Drain
# Get list of files with NERSCClean set
# foreach file in that list
#   check if local file exists
#     if yes, execute a delete
#     reset the status to LocalDeleted
def Phase5():
    #
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetfindbundles + U.mangle('NERSCClean'))
    answer1, erro1, code1 = U.getoutputerrorsimplecommand(geturl, 1)
    answer = U.massage(answer1)
    if 'DOCTYPE HTML PUBLIC' in answer or 'FAILURE' in answer:
        print('Phase 5 failure with', geturl, answer, erro1, code1)
        return
    if len(answer) == 0:
        return	# Nothing to do
    jjanswer = json.loads(U.singletodouble(answer))
    numwaiting = len(jjanswer)
    # Sanity check
    if numwaiting <= 0:
        # This should not happen, but maybe the json isn't understood
        print('Phase 5 json is empty', str(answer))
        return
    #
    for js in jjanswer:
        try:
            localname = str(js['localName'])
        except:
            print('Phase 5: problem with getting info from', js)
            continue
        try:
            if localname[-4:] != '.zip':
                continue
        except:
            print(localname, 'is not a .zip bundle')
            continue
        if localname.find(' ') > 0 or localname.find('\t') > 0:
            print(localname, 'has dangerous blank spaces in the name')
            continue
        try:
            command = ['/usr/bin/ls', localname]
            outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
            if int(code) == 0:
                command = ['/usr/bin/rm', localname]
                outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
                if int(code) != 0:
                    print('Failed to delete', localname, outp, erro)
                    continue
        except:
            print('Phase 5: I do not see the file, or else deleting it fails', localname)
            continue
        key = js['bundleStatus_id']
        outp, erro, code = U.flagBundleStatus(key, 'LocalDeleted')
        if len(outp) > 0:
            print('Phase 5: failed to set status=LocalDeleted for', localname, outp, erro, code)
            continue 
    return
#
###
# Main
Phase0()
if DEBUGIT:
    print('Done w/0, checking space usage')
Phase1()
if DEBUGIT:
    print('Done w/1, checking for jade failed transfers')
Phase2()
if DEBUGIT:
    print('Done w/2 checking for jade completed transfers')
Phase3()
if DEBUGIT:
    print('Done w/3 checking for new local files to transfer')
Phase4()
if DEBUGIT:
    print('Done w/4 submitting new files to jade')
#Phase5()
# Phase5 does not work unless we have write access to the bundles'
#  scratch filesystem, and jade-lta does not.
#print('Done w/5 deleting local copies of successful transfers')
