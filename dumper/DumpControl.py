# DumpControl.py.base
''' Manage information about tapes in slots on jade03; submit dump jobs '''
import sys
import datetime
import json
import copy
import os
import Utils as U

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
    ''' Generate unique list of upper directories:
         e.g. ['Icecube/2018/unbiased/PFRaw/0102', 'IceCube/2018/unbiased/PFRaw/0103'] 
         returns ['IceCube/2018/unbiased/PFRaw']    '''
    #+
    # Arguments:	list of directory names
    # Returns:		list of unique tree-tops from the argument
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    if len(desiredtrees) <= 0:
        return []
    toplist = []
    for tree in desiredtrees:
        tip = tree.split('/')[0]
        if tip not in toplist:
            toplist.append(tip)
    return toplist

###
#  Count the number of files in the specified directory and compare
#   with the expected number
def CountFilesInDir(cfidname, cfidexpectedcount):
    ''' Count the files (beware of subdirectories!) in this directory,
         compare with expected number '''
    #+
    # Arguments:	directory name to look in
    # Returns:		boolean:  True if counts= expected, else False
    # Side Effects:	print and die if the directory isn't visible
    # Relies on:	FLAT DIRECTORY structure, all files at this level
    #			 with no subdirectories
    # NOTE:  May want to replace with a find in case I do need to traverse
    #         trees
    #-
    cfidcommand = ['/bin/ls', cfidname]
    cfidoutp, cfiderro, cfidcode = U.getoutputerrorsimplecommand(cfidcommand, 1)
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
    ''' Determine what is in the removable disk slot '''
    #+
    # Arguments:	path to slot's directory e.g. /mnt/slot6
    # Returns:		[diskuuid, list of compositely named directories to view (simple views),
    #                    detailed list for lower nested, fragment (w/o slot) compositely named dirs
    #			 fragments (w/o slot) of detailed directories
    #			 based on wanted trees:
    #			 e.g. /mnt/slot6/ARA/YEAR/unbiased/SPS-NUPHASE
    #			 "YEAR" is replaced by what is found in the directory
    #			 There may be 2 different years on view in one disk!
    # Side Effects:	print message on error
    #			multiple attempts to read the filesystem
    # Relies on:	REST server working
    #			GiveTops
    #			RetrieveDesiredTrees
    #-		
    # First find out what trees we want to read
    desiredtrees = U.RetrieveDesiredTrees()
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
    i2outp, i2erro, i2code = U.getoutputerrorsimplecommand(command, 1)
    if int(i2code) != 0 or len(i2outp) <= 1:
        print('Cannot read the disk in', slotlocation, i2erro)
        return []       # Do I want to set an error?
    answer = str(i2outp)
    for toplevel in answer.split():
        #print(toplevel)
        if len(toplevel.split('-')) == 5:
            diskuuid = toplevel
    #
    # Generate the list of directories to rsync
    dirstoscan = []
    detaildirs = []
    ylist = []
    for tip in toplist:
        if tip in answer.split():
            command = ['/bin/ls', slotlocation + '/' + tip]
            i3outp, _, i3code = U.getoutputerrorsimplecommand(command, 1)
            if int(i3code) != 0:
                continue        # May not be present, do not worry about it
            tipyearlist = []
            for y in i3outp.split():
                if y not in ylist:
                    ylist.append(y)
                tipyearlist.append(y)
            for dt in desiredtrees:
                if tip in dt:
                    words = dt.split('YEAR')
                    for y in tipyearlist:
                        dirstoscan.append(words[0] + y + words[1])
    #
    for vdirs in desiredtrees:
        words = vdirs.split('YEAR')
        for y in ylist:
            vtop = slotlocation + '/' + words[0] + y + words[1]
            command = ['/bin/ls', vtop]
            i3outp, _, i3code = U.getoutputerrorsimplecommand(command, 1)
            if int(i3code) != 0 or len(i3outp) <= 0:
                continue        # May not be present, do not worry about it
            subdirs = str(i3outp).split()
            for subdir in subdirs:
                detaildirs.append(vtop + '/' + subdir)
    detailfrags = []
    for d in detaildirs:
        detailfrags.append(d.split(slotlocation)[1])
    # yes, there's a duplicate.  Refactoring artificat, clean later
    return [diskuuid, dirstoscan, detaildirs, dirstoscan, detailfrags]

####
# Get the UUID, if this is readable, for use with slot
# management
def InventoryOne(slotlocation):
    ''' Get the UUID, if this location is readable, for use keeping
        track of slot use '''
    #+
    # Arguments:	location of the slot.  e.g. /mnt/slot6
    # Returns:		UUID of disk (exists as a file in top directory)
    # Side Effects:	print error if failure
    #			Does an ls of the slot (1-second timeout)
    # Relies on:	disk has the usual jade format
    #-
    #
    command = ['/bin/ls', slotlocation]
    i1outp, i1erro, i1code = U.getoutputerrorsimplecommand(command, 1)
    if int(i1code) != 0 or len(i1outp) <= 1:
        print('Cannot read the disk in', slotlocation, i1erro)
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
    ''' Set the poleID for the given slot in the database '''
    #+
    # Arguments:	slotnumber (1-12)
    #			pole id number (ID for that disk entry)
    # Returns:		Nothing
    # Side Effects:	Changes database
    # Relies on:	REST server working
    #-
    case = U.mangle(str(slotnum) + ' ' + str(poleidnum))
    ssposturl = copy.deepcopy(U.basicposturl)
    ssposturl.append(U.targetdumpingsetslotcontents + case)
    s1outp, s1err, s2code = U.getoutputerrorsimplecommand(ssposturl, 1)
    if len(s1outp) != 0 or s2code != 0:
        print('SetSlotsPoleID failed', case, s1err)
        sys.exit(0)


####
# Go through all the slots and see what the UUID is in them
# Connect information between SlotContents and PoleDisk
#  entries
def InventoryAll():
    ''' View all the slots available and check their UUIDs
         Load the resulting info in SlotContents and PoleDisk tables '''
    #+
    # Arguments:	None
    # Returns:		Nothing
    # Side Effects:	print and die for failure
    #			print for empty
    #			Changes in REST server database
    # Relies on:	REST server working
    #			InventoryOne
    #			SetSlotsPoleID
    #-
    i1geturl = copy.deepcopy(U.basicgeturl)
    i1geturl.append(U.targetdumpingslotcontents)
    i1outp, i1erro, i1code = U.getoutputerrorsimplecommand(i1geturl, 1)
    if int(i1code) != 0:
        print('SlotContents failure', i1geturl, i1outp, i1erro)
        sys.exit(0)
    my_json = json.loads(U.singletodouble(i1outp))
    i1geturl = copy.deepcopy(U.basicgeturl)
    i1geturl.append(U.targetdumpingdumptarget)
    i1outp, i1erro, i1code = U.getoutputerrorsimplecommand(i1geturl, 1)
    if int(i1code) != 0 or 'FAILURE' in str(i1outp):
        print('get dump target failure', i1geturl, i1outp)
        sys.exit(0)
    targetareaj = json.loads(U.singletodouble(str(i1outp)))[0]
    targetarea = targetareaj['target']
    #
    for js in my_json:
        # First check if the slot is disabled
        if js['poledisk_id'] < 0:
            continue
        # Now inventory the slot--get the UUID of the disk, if available
        diskuuid = InventoryOne(js['name'])
        if diskuuid in ('', []):
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
        i1posturl = copy.deepcopy(U.basicposturl)
        i1posturl.append(U.targetdumpingpolediskloadfrom + U.mangle(stuffforpd))
        i2outp, i2erro, i2code = U.getoutputerrorsimplecommand(i1posturl, 1)
        if int(i2code) != 0 or 'FAILURE' in str(i2outp) or '404 Not Found' in str(i2outp):
            print('Load new PoleDisk failed', i1posturl)
            print(i2outp, i2erro, i2code)
            print(stuffforpd)
            sys.exit(0)
        #
        # OK, now I want to read back what I wrote so I get the new poledisk_id
        i2geturl = copy.deepcopy(U.basicgeturl)
        i2geturl.append(U.targetdumpingpolediskuuid + U.mangle(diskuuid))
        i2outp, i2erro, i2code = U.getoutputerrorsimplecommand(i2geturl, 1)
        if len(i2outp) == 0 or 'FAILURE' in str(i2outp):
            print('Retrive PoleDisk info failed', i2geturl, i2outp, i2erro, i2code)
            sys.exit(0)
        soutp = i2outp
        try:
            djson = json.loads(U.singletodouble(soutp))
            js = djson[0]
            cslot = str(js['slotnumber'])
            cpole = str(js['poledisk_id'])
        except:
            print('Retrieve of new poledisk_id failed', soutp)
            sys.exit(0)
        SetSlotsPoleID(cslot, cpole)

#########################################################
# JOB CHECK CODE
def JobInspectAll():
    ''' Check for activity '''
    #+
    # Arguments:	None
    # Returns:		list of arrays of dump info
    #			 [diskuuid, slot#, datestarted, pole disk id, slot name]
    #			list of arrays of info from ps that match job names
    #			list of arrays of dump info for still-running jobs
    #			list of arrays of dump info for not-running jobs (done or dead)
    # Side Effects:	ps executed
    # Relies on:	REST server working
    #-
    # First get a list of all the nominally active slots
    jigeturl = copy.deepcopy(U.basicgeturl)
    jigeturl.append(U.targetdumpinggetactive)
    jioutp, jierro, jicode = U.getoutputerrorsimplecommand(jigeturl, 1)
    if int(jicode) != 0:
        print('JobInspectAll: active slots check failure', jigeturl, jioutp, jierro)
        sys.exit(0)
    try:
        my_json = json.loads(U.singletodouble(jioutp))
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
    #  equal to or higher than U.DUMPING_LIMIT,
    #  don't bother doing anything.
    #  Inspecting the dateBegun vs today might be useful, though
    #
    # Now find out what jobs are active
    #commandj = ['/usr/bin/ps', 'aux']
    commandj = ['/bin/ps', 'aux']
    listing, jerro, jcode = U.getoutputerrorsimplecommand(commandj, 1)
    if int(jcode) != 0:
        print('JobInspectAll: pstree failed', listing, jerro, jcode)
        sys.exit(0)
    candidate = []
    wlisting = str(listing).split()
    for line in wlisting:
        if 'U.DUMPING' in line:
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
    ''' Check if log space is getting full '''
    #+
    # Arguments:	None
    # Returns:		boolean; True if <90% full
    # Side Effects:	df on filesystem
    # Relies on:	Nothing
    #-
    #commandc = ['/usr/bin/df', '-h', U.DUMPING_LOG_SPACE]
    commandc = ['/bin/df', '-h', U.DUMPING_LOG_SPACE]
    coutp, cerro, ccode = U.getoutputerrorsimplecommand(commandc, 1)
    if int(ccode) != 0:
        print('CheckLogSpace failed', coutp, cerro, ccode, U.DUMPING_LOG_SPACE)
        sys.exit(0)
    percent = int(coutp.split()[-2].split('%')[0])
    return percent < 90

####
# Flag completed jobs as needed.  Sanity checking
def JobDecisionCompleted(notmatched):
    ''' Flag the completed jobs, check '''
    #+
    # Arguments:	list of arrays of info about jobs no longer running
    #			 [diskuuid, slot#, datestarted, pole disk id, slot name]
    # Returns:		list of arrays of info about jobs that completed
    # Side Effects:	print and die for certain problems, e.g. job looks like
    #			 it died.  This requires operator inspection!
    #			tries to read log files
    # Relies on:	REST server working
    #			log files not purged
    #			log file names have standardized format
    #-
    # Look for completed jobs and flag them
    # First sanity check
    donelist = []
    if len(notmatched) == 0:
        return donelist
    #
    for jdone in notmatched:
        # First make sure nothing is wrong
        # I expect a script in U.DUMPING_MASTER_LOG_SPACE/U.DUMPING_${UUID} and a log file
        # in U.DUMPING_MASTER_LOG_SPACE/U.DUMPING_${UUID}.log
        # Note that the master logs are short, just holding the commands that
        # were executed.
        jid = jdone[3]
        tentative = U.DUMPING_MASTER_LOG_SPACE + 'U.DUMPING_' + jdone[0] + '.log'
        commandj = ['/usr/bin/tail', '-n1', tentative]
        answerline, jerro, jcode = U.getoutputerrorsimplecommand(commandj, 1)
        if int(jcode) != 0:
            print('JobDecisionCompleted failed to tail', tentative, 'X', jerro)
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
        jdposturl = copy.deepcopy(U.basicposturl)
        jdposturl.append(U.targetdumpingpolediskdone + U.mangle(str(jid)))
        jdoutp, jderro, jdcode = U.getoutputerrorsimplecommand(jdposturl, 1)
        if int(jdcode) != 0 or 'FAILURE' in str(jdoutp):
            print('JobDecisionCompleted: Set status of PoleDisk failed', jid, jderro)
            sys.exit(0)
        donelist.append(jdone)
    # Done flagging completed jobs
    return donelist

###
# Set the PoleDisk status
def SetPoleDiskStatus(spid, sstatus):
    ''' Set the Pole Disk status '''
    #+
    # Arguments:	pole disk ID (in REST database)
    #			new status
    # Returns:		Nothing
    # Side Effects:	Print and die if it fails
    # Relies on:	REST server working
    #-
    if sstatus not in U.PoleDiskStatusOptions:
        print('SetPoleDiskStatus:  bad status', sstatus)
        sys.exit(0)
    sarg = U.mangle(str(spid) + ' ' + sstatus)
    spposturl = copy.deepcopy(U.basicposturl)
    spposturl.append(U.targetdumpingpoledisksetstatus + sarg)
    spoutp, sperro, spcode = U.getoutputerrorsimplecommand(spposturl, 1)
    if int(spcode) != 0 or 'FAILURE' in str(spoutp):
        print('SetPoleDiskStatus: Set status failed', sarg, sperro)
        sys.exit(0)

###
# Get the expected count from the DB
def GetExpectedCount(trialdirname):
    ''' Given the directory, how many files do we expect to find in it? '''
    #+
    # Arguments:	directory fragment
    # Returns:		number expected, or -1 if there was a problem
    # Side Effects:	None
    # Relies on:	REST server working
    #			"expected" table is already populated with the
    #			 expected file count
    #			NOTE:  not trivially retrieved from Pole DB; warehouser
    #			 would know but we aren't warehousing these things
    #-
    #  This checks whether the fragment matches anything in the DB
    # The information is for the first one found!
    gegeturl = copy.deepcopy(U.basicgeturl)
    gegeturl.append(U.targetdumpinggetexpected + U.mangle(trialdirname))
    geoutp, geerro, gecode = U.getoutputerrorsimplecommand(gegeturl, 1)
    if gecode != 0 or len(geoutp) <= 0:
        return -1
    try:
        gejson = json.loads(U.singletodouble(geoutp))
    except:
        print('No expected counts for directory', trialdirname, geerro)
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
    ''' Is any of the recently dumped directories full yet? '''
    #+
    # Arguments:	list of arrays of dump information
    #			[uuid,slot#,datebegun,poledisk_id,slotname]
    # Returns:		Nothing
    # Side Effects:	Changes in REST server DB
    #			print messages on failure
    # Relies on:	REST server working
    #			GiveTarget
    #			InventoryOneFull
    #			GetExpectedCount
    #			CountFilesInDir
    #-
    # Each entry in the array is an array with [uuid,slot#,datebegun,poledisk_id,slotname]
    dcok = []
    if len(donelist) <= 0:
        return
    dtargetdir = U.GiveTarget()
    for packet in donelist:
        # This repeats an earlier disk access!
        dstuff = InventoryOneFull(packet[4])
        for tentativedir in dstuff[4]:
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
                dcposturl = copy.deepcopy(U.basicposturl)
                dcposturl.append(U.targetdumpingenteredreadydir + U.mangle(dtarg))
                dcoutp, dcerro, dccode = U.getoutputerrorsimplecommand(dcposturl, 1)
                if dccode != 0 or 'FAILURE' in str(dcoutp):
                    print('DirectoryCheckFull could not load', dtarg, dcerro)
                    continue
    return

###
# Load

####
# Decide whether a new job is needed, or whether an old job is done
#  Do some cleanup too
def JobDecision(dumperstatus, jdumpnextAction):
    ''' Decide whether a new job is needed, or whether an old job is done '''
    #+
    # Arguments:	status of dumper system
    #			next action dumper is to do
    # Returns:		Nothing
    # Side Effects:	change in REST server DB state
    #			print message on surprises
    #			print and die on failures
    # Relies on:	REST server working
    #			JobInspectAll
    #			JobDecisionCompleted
    #			DirectoryCheckFull
    #			CheckLogSpace
    #			InventoryOneFull
    #			GiveTarget
    #			SetPoleDiskStatus
    #			DumperSetState
    #-
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
        if minutes > U.DUMPING_TIMEOUT:
            print('JobDecision:  job for', jdrunning[0], 'has run long')
            # Don't bail, this might be OK
    #
    # What should we be doing?
    if jdumpnextAction == 'Pause' or dumperstatus == 'Idle':
        return
    #
    # See if the jobcount is low enough to let me add another dump
    if len(candidate) >= U.DUMPING_LIMIT:
        return
    #
    # Got room for another...
    if not CheckLogSpace():
        print('JobDecision:  log space running short')
        return
    # Pick up the next one
    jdgeturl = copy.deepcopy(U.basicgeturl)
    jdgeturl.append(U.targetdumpinggetwaiting)
    jdoutp, jderro, jdcode = U.getoutputerrorsimplecommand(jdgeturl, 1)
    if int(jdcode) != 0 or 'FAILURE' in str(jdoutp):
        print('Get next undone disk failed', jdoutp, jderro)
        sys.exit(0)
    if len(jdoutp) <= 2:
        return		# Nothing left to do here
    my_json = json.loads(U.singletodouble(jdoutp))
    try:
        juuid = my_json[0]['diskuuid']
        jid = my_json[0]['poledisk_id']
        slotnumber = my_json[0]['slotnumber']
    except:
        print('JobDecision:  cannot unpack next disk', my_json)
        sys.exit(0)
    #
    #commandx = ['/usr/bin/cp', U.DUMPING_SCRIPTS + 'dumpscript', U.DUMPING_LOG_SPACE + 'U.DUMPING_' + str(juuid)]
    commandx = ['/bin/cp', U.DUMPING_SCRIPTS + 'dumpscript', U.DUMPING_MASTER_LOG_SPACE + 'U.DUMPING_' + str(juuid)]
    xoutp, xerro, xcode = U.getoutputerrorsimplecommand(commandx, 1)
    if int(xcode) != 0:
        print('JobDecision: failed to copy to new script', xoutp, xerro)
        sys.exit(0)
    jdgeturl = copy.deepcopy(U.basicgeturl)
    jdgeturl.append(U.targetdumpingslotcontents)
    jdoutp, jderro, jdcode = U.getoutputerrorsimplecommand(jdgeturl, 1)
    if int(jdcode) != 0:
        print('JobDecision: SlotContents failure', jdgeturl, jdoutp)
        sys.exit(0)
    sl_json = json.loads(U.singletodouble(jdoutp))
    for js in sl_json:
        if js['slotnumber'] == slotnumber:
            slotlocation = js['name']
            break
    returnstuff = InventoryOneFull(slotlocation)
    if len(returnstuff) < 2:
        # nothing here--not sure why
        SetPoleDiskStatus(jid, 'Error')
        return
    targetdir = U.GiveTarget()
    dirstoscan = returnstuff[1]
    if len(dirstoscan) == 0:
        # nothing we want here, call it done.
        SetPoleDiskStatus(jid, 'Done')
        return
    commandy = [U.DUMPING_SCRIPTS + 'submitdumper', U.DUMPING_MASTER_LOG_SPACE + 'U.DUMPING_' + str(juuid)]
    for source in dirstoscan:
        commandy.append(slotlocation + '/' + source)
        # Create the target directory...
        reducedsource = os.path.dirname(source)
        commandy.append(targetdir + '/' + reducedsource)
    youtp, yerro, ycode = U.getoutputerrorsimplecommand(commandy, 1)
    if int(ycode) != 0:
        print('JobDecision: submitdumper failed ', youtp, yerro)
        sys.exit(0)
    #
    # Update PoleDisk info
    jdposturl = copy.deepcopy(U.basicposturl)
    jdposturl.append(U.targetdumpingpolediskstart + U.mangle(str(jid)))
    jdoutp, jderro, jdcode = U.getoutputerrorsimplecommand(jdposturl, 1)
    if int(jdcode) != 0 or 'FAILURE' in str(jdoutp):
        print('JobDecision:  update PoleDisk w/ start time failed', jdoutp)
        sys.exit(0)
    # If we're running in DumpOne mode, Pause 
    if jdumpnextAction == 'DumpOne':
        U.DumperSetState('Pause')
    #
    #
    return


###########
# MAIN
#
# Should we be active at all?
#  Note that Pause still lets us check whether old jobs have completed.
dumpstatus, dumpNextAction = U.DumperTodo()
if dumpNextAction == 'Inventory':
    # Do not start dumping immediately after inventory!
    InventoryAll()
    U.DumperSetState('Idle')
    dumpstatus = 'Idle'
    U.DumperSetNext('Pause')
    dumpnextAction = 'Pause'

# Note that the Idle status still allows us to check whether old
# jobs have completed.
if dumpstatus in ('Error', 'Inventorying'):
    sys.exit(0)

if dumpNextAction == 'Dump' and dumpstatus == 'Idle':
    U.DumperSetState('Dumping')
    dumpstatus = 'Dumping'

jobinformation = JobInspectAll()
JobDecision(dumpstatus, dumpNextAction)
