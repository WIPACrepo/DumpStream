# ManualFiles.py (.base)
import json
import copy
import Utils as U

DEBUGIT = False
########################################################
#
def Phase1():
    #
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetfindbundles + U.mangle('LocalDeleted'))
    answer1, erro1, code1 = U.getoutputerrorsimplecommand(geturl, 1)
    answer = U.massage(answer1)
    if 'DOCTYPE HTML PUBLIC' in answer or 'FAILURE' in answer:
        print('Phase 1 failure with', geturl, answer, erro1, code1)
        return
    if len(answer) == 0:
        return	# Nothing to do
    #
    jjanswer = json.loads(U.singletodouble(answer))
    numwaiting = len(jjanswer)
    # Sanity check
    if numwaiting <= 0:
        # This should not happen, but maybe the json isn't understood
        print('Phase 1 json is empty', str(answer))
        return
    #
    stuffToLookAt = []
    for js in jjanswer:
        try:
            localname = js['localName']
            loosefilesexpectedpath = os.path.split(js['idealName'])[0]
            bid = js['bundleStatus_id']
        except:
            print('Phase 1: problem with getting info from', js)
            continue
        #print('Candidate for removal', localname)
        stuffToLookAt.append([loosefilesexpectedpath, localname, bid])
    if len(stuffToLookAt) == 0:
        return	#Nothing useful to do
    #
    stuffFound = []
    for dinfo in stuffToLookAt:
        directory = dinfo[0]
        command = ['/usr/bin/ls', directory]
        try:
            outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
            if len(outp) == 0:
                continue
            stuffFound.append([dinfo, outp])
        except:
            print('Failure doing ls of ', directory, command, erro, code, len(outp))
            continue
    #
    if len(stuffFound) <= 0:
        return	# Nothing useful to do
    for pair in stuffFound:
        print(pair[0][0], len(pair[1].split()))		# Print ideal name, not local
    #
    shortanswer = input('OK to remove the above? y/Y ').lower()
    if len(shortanswer) <= 0:
        return			# Don't do anything
    if shortanswer[0] != 'y':
        return			# Don't do anything
    # pair[0][0] is the directory name from which we bundled the loose files 
    # pair[0][1] is the local zip file name, including the local path.  It
    #            may not exist anymore locally.
    # pair[0][2] is the bundle id
    # pair[1] is a list of file names in the directory of loose files.  There
    #            is no path associated, just the file names
    for pair in stuffFound:
        bunchofiles = pair[1].split()
        for fname in bunchofiles:
            command = ['/usr/bin/rm', pair[0][0] + '/' + fname.strip()]
            #print(command)
            try:
                outp, erro, code = U.getoutputerrorsimplecommand(command, 1)
            except:
                print('Phase 1: problem executing command', command)
                continue
            if int(code) != 0:
                print('Phase 1: bad code executing command', command, code)
                continue
        #
        outp, erro, code = U.flagBundleStatus(pair[0][2], 'LocalFilesDeleted')
        if len(outp) > 0:
            print('Phase 1: failed to set status=LocalFilesDeleted for', localname, outp)
            continue
    #
    # I could do a rm -rf pair[0], but it makes me nervous--a DB
    # corruption could delete an awful lot of stuff.  File by file
    # is probably safer--and it should fail if it hits a directory.
    # Slow but safe.
    return

###############
# Main

Phase1()
# Phase1 does not work unless we have write access to the /data/exp/wherever
# filesystem to which the files were initially dumped, and jade-lta does not.
# Nor does the jade account have privileges to delete dumped files.
