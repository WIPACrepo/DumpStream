# ManualBundle.py (.base)
import json
import copy
import Utils as U

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

###############
# Main

Phase5()
# Phase5 does not work unless we have write access to the bundles'
#  scratch filesystem, and jade-lta does not.
