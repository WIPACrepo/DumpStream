# AutoFiles.py
'''
	Look for "finished" bundles and see which have the bundle files still
	present.
	Go the other way:  look for FullDirectories that aren't cleaned up
	 FullDirectories uses the idealName for the directory
	 WorkingTable uses the realDir
	 I ABSOLUTELY MUST USE realDir
	Using that info, check which have transfers established
	Using that info, check which bundles exist
	Using that info, check which bundles are "finished"
	Using that info, check whether the directory has files or not
        Double-check the name of the directory.  Make sure it does not
         have anything to do with the /data/exp tree
        Delete the files in that directory
'''
import os
import sys
import json
import copy
import requests
import Utils as U


DEBUGIT = False
########################################################
#
def GetFullDirsDone():
    ''' Get a list of pairs of ideal/real directory names where toLTA=2
        toLTA=2 means handed off to LTA system.  toLTA=3 means cleaned up 
        Skip directories not found in a dump area, for safety '''
    #+
    # Arguments:	None
    # Returns:		list of [idealName, realName] pairs for the toLTA=2 directories
    # Side Effects:	Print if failure (keeps going w/o failed one)
    # Relies on:	My REST server working
    #			Working with /data/exp dumps!
    #-
    # Request everything handed off, and parse it for the handed-off
    # "exp" should get pretty much anything in we want
    splitterWord = '/exp/'
    combin = U.mangle(splitterWord[0:-1]) + ' 2'
    bulkReceive = requests.get(U.curltargethost + 'dumping/handedoffdir/' + combin)
    bulkList = eval(str(bulkReceive.text))
    breturn = []
    if len(bulkList) <= 0:
        return breturn
    breturn = []
    #
    for chunk in bulkList:
        dname = chunk['idealDir']
        match = MatchIdealToReal(dname)
        if match == '':
            continue	# Don't monkey with the real /data/exp!!!
        breturn.append([dname, match])
    return breturn

####
#
class BearerAuth(requests.auth.AuthBase):
    ''' Utility class for using the token with requests package '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


####
#
def MatchIdealToRealDir(idealDir):
    ''' Find the matching realDir for the idealDir, or blank
        if this is in the real /data/exp and not a link to
        a safe dumping area '''
    #+
    # Arguments:	ideal directory name
    # Returns:		real directory name, if unique
    #			FAILED NONE if no match
    #			FAILED DUPLICATE if multiple matches
    # Side Effects:	Print if failure
    # Relies on:	My REST server working
    #-
    # Get a list of dump targets
    # Get the real path for the directory given
    # If one of the list of dump targets is in the real path
    #  we have a safe area for deletion, return the real path
    # If not, this isn't safe, print an error and return ''
    dumpTargets = U.GiveTarget()
    try:
        realPath = os.path.realpath(idealDir)
    except:
        print('MatchIdealToReal failed to find a real path for', idealDir)
        return ''
    for targ in dumpTargets:
        if targ in realPath:
            return realPath
    print('MatchIdealToReal WARNING:  ', idealDir, 'does not appear to be in a dump target area')
    return ''

####
#
def GetLTAToken():
    ''' Get the token for the LTA REST server '''
    #+
    # Arguments:	None
    # Returns:		string token for server
    # Side Effects:	opens/reads/closes the token file
    # Relies on:	File exists and is readable
    #-
    tokenFile = '/Users/jbellinger/Dump/DumpStream/jadetools/service-token'
    try:
        filen = open(tokenFile, 'r')
        token = filen.readline()
        filen.close()
        return token
    except:
        print('GetLTAToken failed to open and read ', tokenFile)
    return ''


####
#
def GetAllTransfer(token, listOfDirectories):
    ''' Get the full list of transfers from LTA that match the given list of directories '''
    #+
    # Arguments:	LTA server token
    #			list of pairs of [ideal,real] directories to review
    # Returns:		array of [realDir, [array of transfer UUID]] pairs
    #                   There may be multiple transfer requests for a single directory
    #			Only when all are done or exported do we delete
    # Side Effects:	reads LTA server
    # Relies on:	LTA REST server working
    #-
    if len(listOfDirectories) <= 0:
        return ''
    #
    allTransferRequests = requests.get('https://lta.icecube.aq/TransferRequests', auth=BearerAuth(token))
    returnList = []
    for direct in listOfDirectories:
        uuidset = []
        for entry in allTransferRequests.json()['results']:
            if direct[0] == entry['path'] or direct[1] == entry['path']:
                uuidset.append(entry['uuid'])
        if len(uuidset) <= 0:
            continue		# nothing matches, nothing to do
        returnList.append([direct[1], uuidset])
    return returnList

####
#
def AreTransfersComplete(token, infoRow):
    ''' Go through the transfer requests and get the bundle info
        for each.  Find the bundle status for each.  If all are
        external or finished, return true; else false '''
    #+
    # Arguments:	LTA server token
    #			array of [real-path, [array of uuid of transfer requests]]
    # Returns:		Boolean
    # Side Effects:	multiple accesses to LTA REST server
    #			Print if error
    # Relies on:	LTA REST server working
    #-
    #
    trUUID = infoRow[1]
    if len(trUUID) <= 0:
        return False	# Something is wrong, don't break anything
    # Which bundle states are OK to allow deletion of raw files
    #acceptable = ['external', 'finished', 'deleted', 'source-deleted', 'detached', 'completed']
    acceptable = ['external', 'finished']
    bundleuuid = []
    alldone = True
    for tuuid in trUUID:
        transferRequestData = requests.get('https://lta.icecube.aq/TransferRequests/' + tuuid, auth=BearerAuth(token))
        bundleRequest = requests.get('https://lta.icecube.aq/Bundles?request=' + tuuid, auth=BearerAuth(token))
        trbundle = bundleRequest.json()['results']
        if len(trbundle) <= 0:
            print('AreTransfersComplete: TransferRequest has no bundles', transferRequestData.text)
            continue
        for uu in trbundle:
            bundleuuid.append(uu)
        #
    for uu in bundleuuid:
        bundleStatus = requests.get('https://lta.icecube.aq/Bundles/' + uu, auth=BearerAuth(token))
        stat = bundleStatus.json()['status']
        if stat not in acceptable:
            return False
    return True
