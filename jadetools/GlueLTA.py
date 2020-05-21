# GlueLTA.py
'''  Check whether the Dump has created full directories, and start
      condor jobs to load them into the LTA database and Picker/Bundler
      them.  Dump works with Pole disks which have ideal names in the
      /data/exp tree.
'''
import json
import subprocess
import socket
import copy
import os
import sys
import glob
import Utils as U

####
# Set some global stuff with default values
config_file = '/home/jadelta/Glue.json'
YEAR = '2018'
ROOT = '/mnt/lfs7/exp'
PARTIAL = False
SCAN_ONLY = False
SUB_TREES = []
FORCE = False
FORCE_LIST = []
FORBID = False
FORBID_LIST = []
CROOT = '/tmp'
CONDOR_LIMIT = 10
INITIAL_DIR = '/home/jadelta/dumpcontrol/DumpStream/jadetools'

DEBUG = False

def SetStatus(gnewstatus):
    ''' Set or get the GlueStatus.  Return status or error or empty if set worked '''
    # Finite state system :-)
    # Run means currently running.  Ready means ready to run
    # The lastChangeTime is more for internal debugging than user
    # operations, and is not returned via this interface.
    #+
    # Arguments:  new status or Query
    # Returns:    status if Query
    #             Error if bad argument
    #             '' if change succeeded
    #             Failure + stuff if something failed
    # Side Effects:	Prints error if there was a problem
    #			change in REST server info
    # Relies on:	REST server working
    #-
    if gnewstatus not in ['Pause', 'Run', 'Ready', 'Query']:
        return 'Error'
    gposturl = copy.deepcopy(U.basicposturl)
    gposturl.append(U.targetgluestatus + U.mangle(gnewstatus))
    goutp, gerro, gcode = U.getoutputerrorsimplecommand(gposturl, 1)
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
    return grevised

def SetLastGluePassTime():
    ''' Tell the database the last interface pass has completed '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	changes row in REST server's DB
    #			Prints error if there was a problem
    # Relies on:	REST serve working
    #-
    gposturl = copy.deepcopy(U.basicposturl)
    gposturl.append(U.targetgluetimeset + 'LastGluePass')
    goutp, _, _ = U.getoutputerrorsimplecommand(gposturl, 1)
    if 'FAIL' in str(goutp):
        print('SetLastGluePassTime failed to set the last time')

def DiffOldDumpTime():
    ''' Is the most recent dump time newer than the most recent scan?
        Is there a --do-anyway flag set? '''
    #+
    # Arguments:	None
    # Returns:		Boolean
    # Side Effects:	Prints error if there was a problem
    # Relies on:	REST server working, and Dump and Interface times set
    #-
    if len(sys.argv) > 1:
        if '--do-anyway' in sys.argv:
            return True
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
    ''' Parse out what the parameters tell this job to do '''
    #+
    # Arguments:	None [REVISIT THIS.  Probably want to take arguments from command line!]
    # Returns:		None
    # Side Effects:	Resets GLOBAL parameters from the configuration file
    #                   Prints message and exit-3 if the reading fails
    # Relies on:	Configuration file available and readable
    #-
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
    global CROOT
    global CONDOR_LIMIT
    global INITIAL_DIR
    config_file = '/home/jadelta/Glue.json'
    try:
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
            CROOT = data['CROOT']
            CONDOR_LIMIT = int(data['CONDOR_LIMIT'])
    except:
        print('ParseParams:  failed to read the config file', config_file)
        sys.exit(3)
    # Now reload over these from the relevant arguments
    #  Or not.
    # For initial testing, don't bother
    #INITIAL_DIR = os.getcwd()





def GetBundleNamesLike(pcarg):
    ''' Retrieve bundles which are like the specified directory '''
    # This is pretty generic.  If you want a bundle, you can give
    # it the whole ideal name.  If you want all the e.g. PFRaw
    # bundles for a year, feed it "/data/exp/IceCube/2018/unbiased/PFRaw"
    # You can get swamped if you are greedy
    #+
    # Arguments:	directory path fragment (be as specific as possible)
    # Returns:		list of ideal name for bundles which match the path fragment
    #			 (NOTE: information taken from MY NERSC transfer chain, not LTA)
    #			  I want to know what has already been done, to exclude it
    # Side Effects:	Print error if there was a problem
    # Relies on:	REST server working
    #-
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetfindbundleslike + U.mangle(pcarg))
    ganswer1, _, _ = U.getoutputerrorsimplecommand(ggeturl, 1)
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

def GetStagedDirsLike(pcarg):
    ''' Retrieve FullDirectories staged directories (to ignore them) '''
    # This just keeps track of what has been done already.  There are
    # 2 entries per directory:  the directory name (ideal) and the
    # flag (in this case >0).  Originally the flag was going to tell
    # whether the directory had been shipped as well as handed off to
    # LTA, but there's no point in shoehorning that additional info in.
    #
    #+
    # Arguments:	directory path fragment (be as specific as possible)
    # Returns:		list of ideal directory names
    #			 for directories that have already been handed off to LTA
    # Side Effects:	Print error if there was a problem
    # Relies on:	REST server working
    #-
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetdumpinghandedoffdir + U.mangle(pcarg))
    ganswer1, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    ganswer = U.massage(ganswer1)
    greturn = []
    if len(ganswer) <= 2 or 'Internal Service Error' in ganswer:
        return []
    try:
        ggans = U.singletodouble(ganswer).replace(' None,', ' \"None\",')
        gjanswer = json.loads(ggans)
        for j in gjanswer:
            greturn.append(str(j['idealName']))
    except:
        print('GetStagedDirsLike failed with', pcarg, ganswer, gerro, gcode)
    return greturn


def GetBundleDirsLike(pcarg):
    ''' Retrieve bundle directories like the specified directory
         Get both from BundleStatus and FullDirectories '''
    #+
    # Arguments:	directory path fragment (be as specific as possible) 
    # Returns:		list of ideal directory names
    #			 for directories that have already been handed off to LTA
    #                    or bundled by my system
    # Side Effects:	None
    # Relies on:	REST server working
    #                    GetBundleNamesLike
    #                    GetStagedDirsLike
    #-
    got_bundles = GetBundleNamesLike(pcarg)
    got_staged_dirs = GetStagedDirsLike(pcarg)
    if len(got_bundles) == 0 and len(got_staged_dirs) == 0:
        return []
    #
    dreturn = []
    for bname in got_bundles:
        cleanname = U.ParseCleanDirName(os.path.dirname(bname))
        if cleanname not in dreturn:
            dreturn.append(cleanname)
    for dname in got_staged_dirs:
        cleanname = U.ParseCleanDirName(dname)
        if cleanname not in dreturn:
            dreturn.append(cleanname)
    return dreturn

def FullToFrag(dname, lYEAR):
    ''' Turn the real or ideal file or directory name into a YEAR-prefixed fragment '''
    #+
    # Arguments:	name, real or ideal, in data warehouse format,
    #			 with "YEAR" as placeholder for the year
    #			name of year to prefix the fragment with
    # Returns:		fragment of name string prefixed with the year
    #			'Failure' if the format isn't right
    # Side Effects:	Print error if the format isn't right
    # Relies on:	Nothing
    #-
    dword = dname.split('/' + str(lYEAR) + '/')
    if len(dword) != 2:
        print('The name is not in ideal warehouse form wrt year', dname, lYEAR)
        return 'FAILURE'
    return ('/' + str(lYEAR) + '/' + dword[1]).replace('//', '/')

def SubdirInList(directory, directoryList, lYEAR) -> bool:
    ''' I want to compare ideal and real directory names.
        Does the given directory match one in the list? '''
    #+
    # Arguments:	generic directory name with YEAR as placeholder
    #			list of ideal directory names already done
    #			actual year to replace placeholder with
    # Returns:		False if 'FAILURE' (fail in favor of processing)
    #			False if the given directory isn't in the done list
    #			True if the given directory _is_ in the done list
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    #
    if len(directoryList) == 0:
        return False
    rdir = FullToFrag(directory, lYEAR)
    #
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
    #+
    # Arguments:	file name
    # Returns:		directory name
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    try:
        pd = os.listdir(d)
    except:
        return []
    if len(pd) <= 0:
        return []
    return [os.path.join(d, f) for f in pd]

def GetExpectedFromFrag(gfrag):
    ''' Get the expected # of files from the expected table for gfrag '''
    #+
    # Arguments:	directory fragment
    # Returns:		-1 if problem
    #			 Number of files expected in the directory
    # Side Effects:	Print error if problem:
    #			 IF the directory is expected to be empty, PROBLEM
    # Relies on:	REST server working
    #-
    ggeturl = copy.deepcopy(U.basicgeturl)
    ggeturl.append(U.targetdumpinggetexpected + U.mangle(gfrag))
    ganswer1, gerro, gcode = U.getoutputerrorsimplecommand(ggeturl, 1)
    ganswer = U.massage(ganswer1)
    if len(ganswer) == 0:
        print('GetExpectedFromFrag: No answer for', gfrag, ganswer, gerro, gcode)
        return -1
    return int(ganswer)

def GetToken():
    ''' Get the token, if possible to let the Dump to LTA interface run : gets 0, 1, 2'''
    #+
    # Arguments:	None
    # Returns:		True if we got the Token OK
    #			False if we didn't
    # Side Effects:	Print error if there was a problem
    #			REST server status change if success
    # Relies on:	REST server working
    #-
    print('JNBDEBUG: GetToken')
    gposturl = copy.deepcopy(U.basicposturl)
    gposturl.append(U.targetgluetoken + U.mangle(socket.gethostname()))
    ganswer, _, _ = U.getoutputerrorsimplecommand(gposturl, 1)
    try:
        gmycode = int(ganswer)
    except:
        print('GetToken failure', ganswer)
        return False
    if gmycode == 1:
        return False
    if gmycode == 2:
        print('GetToken failure code 2')
        return False
    return True

def ReleaseToken():
    ''' Release the token for running '''
    #+
    # Arguments:	None
    # Returns:		True if we released the Token OK
    #			False if we didn't
    # Side Effects:	Print error if there was a problem
    #			REST server status change if success
    # Relies on:	REST server working
    #-
    print('JNBDEBUG: ReleaseToken')
    gposturl = copy.deepcopy(U.basicposturl)
    gposturl.append(U.targetgluetoken + U.mangle('RELEASE'))
    ganswer, _, _ = U.getoutputerrorsimplecommand(gposturl, 1)
    try:
        gmycode = int(ganswer)
    except:
        print('ReleaseToken failure', ganswer)
        return False
    if gmycode == 0:
        return True
    return False



def Phase0():
    ''' Initial program configuration '''
    #+
    # Arguments:	None
    # Returns:		True if we can run
    #			False if we should not
    #			False if there's nothing to do
    # Side Effects:	Print error/warning if there are problems
    #			REST server status change
    # Relies on:	GetToken
    #			ReleaseToken
    #			ParseParams
    #			SetStatus
    #			DiffOldDumpTime
    #-
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
    if not GetToken():
        print('DEBUG Phase0 busy, bailing')
        return False
    ParseParams()
    #
    # Should we do anything?
    run_status = SetStatus('Query')
    if run_status in ['Run', 'Pause'] and not FORCE:
        print('DEBUG Phase0 run_status=', run_status)
        if not ReleaseToken():
            print('Phase0: Failed to release token: A')
        return False
    #
    new_dump = DiffOldDumpTime()
    if not new_dump:
        if not FORCE:
            print('Phase0: forcing release token, no new dump')
            if not ReleaseToken():
                print('Phase0: Failed to release token: C')
            return False
        print('WARNING:  Forcing run w/ old dump')
    #
    answer = SetStatus('Run')
    if answer != '':
        print('Phase0: status return', answer)
        answer = SetStatus('Ready')
        if not ReleaseToken():
            print('Phase 0:  Failed to release token after trying to setStatus')
        return False
    return True

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
    #+
    # Arguments:	None
    # Returns:		list of arrays of live and ideal directories
    #			 for which the file count == expected
    # Side Effects:	Print errors if problem
    # Relies on:	GetBundleDirsLike   (list of done or in process dirs)
    #			listdir_fullpath
    #			SubdirInList	(is this already accounted for)
    #			FullToFrag	(pare down to sufficient fragment)
    #			GetExpectedFromFrag (counts are keyed by directory fragment)
    #-
    if FORCE:
        TODO = copy.deepcopy(FORCE_LIST)
    else:
        TODO = []
    Bulk_tocheck = []
    # Query what the dump thinks are full directories
    unhandledDirs = U.GetUnhandledFull()
    # DEBUG
    for d in unhandledDirs:
        print("DEBUG", d)
    # END DEBUG
    if len(unhandledDirs) == 0:
        return TODO
    for directory in unhandledDirs:
        chunks = directory.split('/exp/')
        partial = '/exp/' + chunks[1]
        done_or_working = GetBundleDirsLike(partial)  # ideal names
        d = directory
        if not SubdirInList(d, FORBID_LIST, YEAR) and not SubdirInList(d, done_or_working, YEAR):
            # A PRIORI knowledge.  This is meant to work with dumps from Pole disks.  If you
            # plan to expand this to other things, that's on you.
            d_Ideal = '/data' + partial
            Bulk_tocheck.append([d, d_Ideal])
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
    for ppair in Bulk_tocheck:
        pdir = ppair[0]
        #print(pdir)
        pcount = len(glob.glob(pdir + '/*'))
        if pcount <= 0:
            continue
        pdir_frag = FullToFrag(pdir, YEAR)
        fcount = int(GetExpectedFromFrag(pdir_frag))
        if pcount == fcount:
            TODO.append(ppair)
            continue
        if pcount > fcount and fcount > 0:
            print('Number of files in ', pdir, 'is greater than expected', fcount, pcount)
        #
    #
    # DEBUG
    for d in TODO:
        print("TODO", d)
    # END DEBUG
    return TODO

def Phase2(lTODO):
    ''' Do the submission stuff here '''
    #+
    # Arguments:	list of arrays of real and ideal directories to bundle
    # Returns:		boolean:  True if OK or nothing to do, False otherwise
    # Side Effects:	change WorkingTable
    #			execute process_directory.sh script
    # Relies on:	REST server working
    #			process_directory.sh script (which relies on LTA environment)
    #-
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
    # Anything to do?
    if len(lTODO) <= 0:
        return True
    for pair_directory in lTODO:
        ldirectory = pair_directory[0]
        print('About to try', ldirectory, flush=True)
        #
        try:
            command = [INITIAL_DIR + '/process_directory.sh', pair_directory[1]]
            output, error, code = U.getoutputerrorsimplecommand(command, 86400)
            #subprocess.run(command, shell=False, timeout=86400, check=True, capture_output=True)
        except subprocess.TimeoutExpired:
            print('Phase2: Timeout on process_directory.sh on', ldirectory)
            return False
        except subprocess.CalledProcessError as e:
            print('Phase2: Failure with process_directory.sh on', ldirectory, 'with', e.stderr, e.output)
            return False
        except:
            print('Phase2: Failed to execute process_directory.sh on', ldirectory, error, code)
            print(command)
            return False
        if code != 0:
            print('Phase2: Problem with process_directory.sh', output, error, code)
        else:
            if 'Error:' in str(output) or 'Error:' in str(error):
                print('Phase2:', ldirectory, output, error, code)
    #
    return True

####
#
#+
# Main sequence: No arguments
# Relies on:	ReleaseToken
#		Phase0  (OK to run?)
#		Phase1  (accumulate stuff to do)
#		Phase2  (do the stuff)
#-
if not Phase0():
    ans_token = ReleaseToken()
    print('Phase0 false')
    sys.exit(0)
mytodo = Phase1()
#print(mytodo)
phase2_ok = Phase2(mytodo)
ans_token = ReleaseToken()
ans_status = SetStatus('Ready')
# Flag that we're done
SetLastGluePassTime()
if not phase2_ok:
    print('phase2_ok false')
    sys.exit(1)
sys.exit(0)
