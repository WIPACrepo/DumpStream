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
        print(goutp)
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
        print('PurgeWork could not empty WorkingTable')

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
        print('UpdateWork failed with ', idir, str(goutp))
    return ''

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
            print('InsertWork failure at chunk ', iw, ' of ', ichunks, ' with ', gposturl, goutp)
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
            SUB_TREES.append(tree)
        FORCE = bool(data['FORCE'])
        FORCE_LIST = []
        FORBID = bool(data['FORBID'])
        FORBID_LIST = []
    # Now reload over these from the relevant arguments
    #  Or not.
    # For initial testing, don't bother


def DebugTesting():
    ''' Some initial testing stuff '''
    if DEBUG:
        dummydirs = ['/data/exp/Superice', '/data/exp/Subice']
        print(GetWorkCount())
        inserted = InsertWork(dummydirs)
        print(GetWorkCount())
        print(PurgeWork())
        print(GetWorkCount())
        xx = UpdateWork(dummydirs[0])
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
            print('WARNING: Forcing run w/ old dump')
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
    file_count = {}
    for p in SUB_TREES:
        parts = p.split('YEAR')
        tentative = ROOT + '/' + YEAR + '/' + parts[1] + '/*'
        subdirlisting = glob.glob(tentative).sort()
        for d in subdirlisting:
            if d not in FORBID_LIST:
                Bulk_tocheck.append(d)
    if len(Bulk_tocheck) <= 0:
        return TODO
    # BFI is ugly.  This represents thousands of calls!
    # Can I pull from my DB to exclude some of these?
    # select status,idealName from BundleStatus where idealName
    #  LIKE p in SUB_TREES and status in (..)  
    # Take the below and put it in the loop above after vetting
    # the individual directories in BundleStatus
    for pdir in Bulk_tocheck:
        pcount = glob.glob(pdir + '/*')
        filecount[pdir] = pcount
    #
    return TODO


####
#
Phase0()
