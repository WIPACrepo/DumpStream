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
import Utils as U

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
        return 'Failure to make json:  bad url?'
    #
    if gnewstatus == 'Query':
        return grevised
    return 'Failure ' + grevised

def Phase0():
    # Parse parameters, if any
    # If we aren't "Forcing" or "Partial" only, should we be running
    #   No if GlueStatus.status is not "Run"
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
    print(SetStatus('Query'))

####
#
Phase0()
