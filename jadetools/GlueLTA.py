# GlueLTA.py
'''  Check whether the Dump has created full directories, and start
      condor jobs to load them into the LTA database and Picker/Bundler
      them
'''
import datetime
import json
import subprocess
import copy
import os
import sys
import glob
import Utils as U

config_file = '/home/jbellinger/Glue.json'
YEAR = '2018'
ROOT = '/mnt/lfs7/exp'
PARTIAL = False
SCAN_ONLY = False
SUB_TREES = []
FORCE = False
FORCE_LIST = []
FORBID = False
FORBID_LIST = []

DEBUG = False

def SetStatus(gnewstatus):
    ''' Set or get the GlueStatus.  Return status or empty '''
    # Finite state system :-)
    # Run means currently running.  Ready means ready to run
    # The lastChangeTime is more for internal debugging than user
    # operations, and is not returned via this interface.
    if gnewstatus not in ['Pause', 'Run', 'Ready', 'Query']:
        return 'Error'
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetgluestatus + U.mangle(gnewstatus))
    goutp, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    if len(goutp) == 0:
        return ''	# all is well
    try:
        gmy_json = json.loads(U.singletodouble(goutp))[0]
        grevised = str(gmy_json['status'])
    except:
        print(goutp, gerro, gcode)
        return 'Failure to make json:  bad url?'
    #
    if gnewstatus == 'Query':
        return grevised
    return 'Failure ' + grevised

def PurgeWork():
    ''' Empty out WorkingTable '''
    gposturl = copy.deepcopy(U.basicposturl)
    gposturl.append(U.targetglueworkpurge)
    goutp, gerro, gcode = U.getoutputerrorsimplecommand(gposturl, 1)
    if 'FAILURE' in goutp:
        print('PurgeWork could not empty WorkingTable', goutp, gerro, gcode)

def GetWorkCount():
    ''' Get the count of 'Unpicked' in WorkingTable '''
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetglueworkcount)
    goutp, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    if len(goutp) == 0:
        print('GetWorkCount failure', gerro, gcode)
        return 0
    try:
        icount = int(goutp)
    except:
        print('GetWorkCount: Failure to unpack return?')
        return -1
    return icount

def UpdateWork(idir):
    ''' Update the directory to have Picked status '''
    gposturl = copy.deepcopy(U.basicposturl)
    gposturl.append(U.targetglueworkupdate + U.mangle(idir))
    goutp, gerro, gcode = U.getoutputerrorsimplecommand(gposturl, 1)
    if len(str(goutp)) > 1:
        print('UpdateWork failed with ', idir, str(goutp), gerro, gcode)

def InsertWork(idir_array):
    ''' Insert the directories in WorkingTable '''
    ilendir = len(idir_array)
    if ilendir <= 0:
        return 0
    # assemble a long string.  Assume we have a limit of 20/per 
    number_inserted = 0
    ichunks = int(ilendir/20) + 1
    for iw in range(ichunks):
        ilow = iw * 20
        ihigh = min(ilendir + 1, ilow + 20)
        istring = ''
        for idir in idir_array[ilow:ihigh]:
            istring = istring  + ' ' + idir
        gposturl = copy.deepcopy(U.basicposturl)
        gposturl.append(U.targetglueworkload + U.mangle(istring))
        goutp, gerro, gcode = U.getoutputerrorsimplecommand(gposturl, 1)
        if 'FAILURE' in str(goutp):
            print('InsertWork failure at chunk ', iw, ' of ', ichunks, ' with ', gposturl, goutp, gerro, gcode)
            breakdown = str(goutp).split()
            number_inserted = number_inserted + int(breakdown[1])
            return number_inserted
        ictest = len(str(goutp).split())
        if ictest > 1:
            print('Insertwork failure at chunk ', iw, ' of ', ichunks, ' having ', goutp, ' from ', gposturl)
            return -1
        number_inserted = number_inserted + int(str(goutp))
    if number_inserted != ilendir:
        print('InsertWork failure: inserted ', number_inserted, ' out of ', ilendir)
    return number_inserted


def DiffOldDumpTime():
    ''' Is the most recent dump time newer than the most recent scan '''
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetgluetimediff)
    goutp, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    if len(goutp) == 0:
        print('DiffOldDumpTime failure', gerro, gcode)
        return False
    if float(str(goutp)) < 0:
        return False
    return True

def ParseParams():
    ''' Parse out what the parameters tell us to do '''
    # These will be globals
    # If not set by the parameters, take these globals from
    #  the configuration file
    # Year = 
    # Config = location of config file
    # Subtrees	= array of subtree names
    # Force = "directory name"	(Do only this one)
    # Forbid = array of directory names (Don't do these from Subtrees)
    # Root = root for dumping files (currently /mnt/lfs7/exp)
    # Partial => write out which directories ARE NOT FULL YET
    # ScanOnly => only write out the directores that would be processed
    #
    # check whether Config is set in the parameters
    #
    # load from the config file
    global config_file
    global YEAR
    global ROOT
    global PARTIAL
    global SCAN_ONLY
    global SUB_TREES
    global FORCE
    global FORCE_LIST
    global FORBID
    global FORBID_LIST
    config_file = '/home/jbellinger/Glue.json'
    with open(config_file) as json_file:
        data = json.load(json_file)
        YEAR = data['YEAR']
        ROOT = data['ROOT']
        PARTIAL = bool(data['PARTIAL'])
        SCAN_ONLY = bool(data['SCAN_ONLY'])
        SUB_TREES = []
        for tree in data['SUB_TREES']:
            SUB_TREES.append(tree['tree'])
        FORCE = bool(data['FORCE'])
        FORCE_LIST = []
        FORBID = bool(data['FORBID'])
        FORBID_LIST = []
    # Now reload over these from the relevant arguments
    #  Or not.
    # For initial testing, don't bother

def GetBundleNamesLike(pcarg):
    ''' Retrieve bundles which are like the specified directory '''
    # This is pretty generic.  If you want a bundle, you can give
    # it the whole ideal name.  If you want all the e.g. PFRaw
    # bundles for a year, feed it "/data/exp/IceCube/2018/unbiased/PFRaw"
    # You can get swamped if you are greedy
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetfindbundleslike + U.mangle('%25' + pcarg + '%25'))
    ganswer1, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    ganswer = U.massage(ganswer1)
    if len(ganswer) <= 2:
        #print('DEBUGGING: GetBundleNamesLike: No answer for', pcarg, ganswer, gerro, gcode)
        return []
    try:
        ggans = U.singletodouble(ganswer).replace(' None,', ' \"None\",')
        gjanswer = json.loads(ggans)
    except ValueError as err:
        print('GetBundleNamesLike: Failed to create json:', pcarg.replace('//', '/'), ganswer, err)
        return []
    except:
        print('GetBundleNamesLike: Failed to create json:', pcarg.replace('//', '/'), ganswer)
        return []
    greturn = []
    for j in gjanswer:
        try:
            if str(j['status']) != 'Abort':
                greturn.append(j['idealName'])
        except:
            print('GetBundleNamesLike: j=', j)
    #
    return greturn

def GetBundleDirsLike(pcarg):
    ''' Retrieve bundle directories like the given directory '''
    got_bundles = GetBundleNamesLike(pcarg)
    if len(got_bundles) == 0:
        return []
    #
    dreturn = []
    for bname in got_bundles:
        if os.path.dirname(bname) not in dreturn:
            dreturn.append(os.path.dirname(bname))
    return dreturn

def FullToFrag(dname, lYEAR):
    ''' Turn the real or ideal name into a YEAR-prefixed fragment '''
    dword = dname.split('/' + str(lYEAR) + '/')
    if len(dword) != 2:
        print('The name is not in ideal warehouse form wrt year', dname, lYEAR)
        return 'FAILURE'
    return ('/' + str(lYEAR) + '/' + dword[1]).replace('//', '/')

def SubdirInList(directory, directoryList, lYEAR) -> bool:
    ''' I want to compare ideal and real directory names.
        Does the given directory match one in the list? '''
    #
    if len(directoryList) == 0:
        return False
    rdir = FullToFrag(directory, lYEAR)
    #DEBUG
    #if directoryList[0] == '/data/exp/IceCube/2018/unbiased/PFRaw/0626':
    #    print(rdir, FullToFrag(directoryList[0], lYEAR))
    if rdir == 'FAILURE':
        return False
    for sdir in directoryList:
        ldir = FullToFrag(sdir, lYEAR)
        if ldir == 'FAILURE':
            return False
        if ldir == rdir:
            return True
    return False

def listdir_fullpath(d):
    ''' Stolen from stackoverflow '''
    try:
        pd = os.listdir(d)
    except:
        return []
    if len(pd) <= 0:
        return []
    return [os.path.join(d, f) for f in pd]

def GetExpectedFromFrag(gfrag):
    ''' Get the expected # of files from the expected table for gfrag '''
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetdumpinggetexpected + U.mangle(gfrag))
    ganswer1, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    ganswer = U.massage(ganswer1)
    if len(ganswer) == 0:
        print('GetExpectedFromFrag: No answer for', gfrag, ganswer, gerro, gcode)
        return -1
    return int(ganswer)
    

def DebugTesting():
    ''' Some initial testing stuff '''
    if DEBUG:
        dummydirs = ['/data/exp/Superice', '/data/exp/Subice']
        print(GetWorkCount())
        inserted = InsertWork(dummydirs)
        print('inserted=', inserted)
        print(GetWorkCount())
        print(PurgeWork())
        print(GetWorkCount())
        UpdateWork(dummydirs[0])
        print(GetWorkCount())


def Phase0():
    ''' Initial program configuration '''
    # Parse parameters, if any
    # If we aren't "Forcing" or "Partial" only, should we be running
    #   No if GlueStatus.status is not "Ready"
    #   No if select count(*) from WorkingTable where status=='Unpicked' > 0
    #   Yes otherwise
    #   If "No" but "Forcing" is enabled, warn
    #   If "No", quit
    # Purge the WorkingTable
    # Fetch the contents of DumpGlueTable (type="LastGluePass" or type=="LastDumpEnd")
    # If the changeTime for LastGluePass > changeTime for LastDumpEnd
    #   Nothing to do, unless Forcing, quit
    # Open/read the config file, assemble the configuration info data structure
    # Return the configuration data structure
    #
    # Test the utilities
    ParseParams()
    #
    # Should we do anything?
    run_status = SetStatus('Query')
    if run_status in ['Run', 'Pause'] and not FORCE:
        sys.exit(0)
    #
    still_in_process = GetWorkCount()
    if still_in_process > 0:
        if not FORCE:
            sys.exit(0)
        else:
            print('WARNING:  Forcing run w/ ongoing work!')
    #
    new_dump = DiffOldDumpTime()
    if not new_dump:
        if not FORCE:
            sys.exit(0)
        else:
            print('WARNING:  Forcing run w/ old dump')
    #
    # Get rid of old stuff in the WorkingTable
    PurgeWork()

def Phase1():
    ''' Get the directories TODO (returned) '''
    # Assemble list of directories to examine (unless Forced)
    # If Forced, initialize TODO with the forced directory, else []
    # Retrieve "Already Picked" and "In Process" for each directory
    #   Create WorkingTable entries from these
    # Skipping those that are done (unless Forced):
    #   Retrieve "expected" count for each directory
    #   Execute "find" in that directory and count the files
    #   If this fails/times-out, log the errors and bail
    #   If they agree, append to the TODO
    #   If # present > # expected, warn
    # Return the TODO
    if FORCE:
        TODO = copy.deepcopy(FORCE_LIST)
    else:
        TODO = []
    Bulk_tocheck = []
    for p in SUB_TREES:
        parts = p.split('YEAR')
        lower_part = (parts[0] + YEAR + parts[1]).replace('//', '/')
        done_or_working = GetBundleDirsLike(lower_part)  # ideal names
        tentative = (ROOT + '/' + parts[0] + '/' + YEAR + '/' + parts[1]).replace('//', '/')  # on-disk
        subdirlisting = listdir_fullpath(tentative)
        #print(tentative)
        if subdirlisting is None:
            print('Nothing for this?')
            continue
        #print(len(subdirlisting))
        for d in subdirlisting:
            if not SubdirInList(d, FORBID_LIST, YEAR) and not SubdirInList(d, done_or_working, YEAR):
                Bulk_tocheck.append(d)
    if len(Bulk_tocheck) <= 0:
        return TODO
    #  Somehow I need to map the real directory into the ideal
    # so I can retrieve the expected counts.
    # BFI is ugly.  This represents thousands of calls!
    # Can I pull from my DB to exclude some of these?
    # select status,idealName from BundleStatus where idealName
    #  LIKE p in SUB_TREES and status in (..)  
    # Take the below and put it in the loop above after vetting
    # the individual directories in BundleStatus
    for pdir in Bulk_tocheck:
        pcount = len(glob.glob(pdir + '/*'))
        if pcount <= 0:
            continue
        pdir_frag = FullToFrag(pdir, YEAR)
        fcount = int(GetExpectedFromFrag(pdir_frag))
        if pcount == fcount:
            TODO.append(pdir)
            continue
        if pcount > fcount and fcount > 0:
            print('Number of files in ', pdir, 'is greater than expected', fcount, pcount)
        #
    #
    return TODO

def Phase2(lTODO):
    ''' Do the submission stuff here '''
    #  if lTODO is empty, just log the run time and quit
    # Create a working list file
    #  [DEFINE DIRECTORY framework for this stuff]
    # NEED A LIMIT FOR THE NUMBER OF JOBS TO SUBMIT AT ONCE!
    # For each directory in lTODO
    #    Double-log info in case of a crash
    #    Append the directory to the working list file
    #    Set the already-picked flag to "in process" and set the date
    #     in WorkingTable
    #    Up until the limit of jobs
    # Create a condor dag job to run these
    # The condor job is responsible for resetting the "already picked"
    #  to "Picked"  This lets me monitor whether condor jobs crashed
    # Record the condor job info in WorkingTable
    #
    return

####
#
Phase0()
mytodo = Phase1()

if DEBUG:
    print(len(mytodo))
    for todo in mytodo:
        print(todo)
