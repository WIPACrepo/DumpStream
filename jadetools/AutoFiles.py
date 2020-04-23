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
import glob
import requests
import Utils as U

#######################################################
#
class BearerAuth(requests.auth.AuthBase):
    ''' Utility class for using the token with requests package '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


########################################################
#
class AutoFiles():
    ''' Class to handle dumped file deletion '''
    def __init__(self):
        ''' __init__ for AutoFiles; load LTA token '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects: Subroutine reads a file
        # Relies on:	GetLTAToken
        #-
        self.token = self.GetLTAToken()
        self.bearer = BearerAuth(self.token)
    #
    def GetFullDirsDone(self):
        ''' Get a list of pairs of ideal/real directory names where toLTA=2
            toLTA=2 means handed off to LTA system.  toLTA=3 means cleaned up 
            Skip directories not found in a dump area, for safety '''
        #+
        # Arguments:	None
        # Returns:		list of [idealName, realName] pairs for the toLTA=2 directories
        # Side Effects:	Print if failure (keeps going w/o failed one)
        # Relies on:	MatchIdealToRealDir
        #			My REST server working
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
            match = self.MatchIdealToRealDir(dname)
            if match == '':
                continue	# Don't monkey with the real /data/exp!!!
            breturn.append([dname, match])
        return breturn
    #
    ####
    #
    def MatchIdealToRealDir(self, idealDir):
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
            print('MatchIdealToRealDir failed to find a real path for', idealDir)
            return ''
        for targ in dumpTargets:
            if targ in realPath:
                return realPath
        print('MatchIdealToRealDir WARNING:  ', idealDir, 'does not appear to be in a dump target area')
        return ''
    #
    ####
    #
    def GetLTAToken(self):
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
    #
    ####
    #
    def GetAllTransfer(self, listOfDirectories):
        ''' Get the full list of transfers from LTA that match the given list of directories '''
        #+
        # Arguments:	list of pairs of [ideal,real] directories to review
        # Returns:	array of [realDir, [array of transfer UUID]] pairs
        #               There may be multiple transfer requests for a single directory
        #		Only when all are done or exported do we delete
        # Side Effects:	reads LTA server
        # Relies on:	LTA REST server working
        #-
        if len(listOfDirectories) <= 0:
            return ''
        #
        allTransferRequests = requests.get('https://lta.icecube.aq/TransferRequests', auth=self.bearer)
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
    #
    ####
    #
    def AreTransfersComplete(self, infoRow):
        ''' Go through the transfer requests and get the bundle info
            for each.  Find the bundle status for each.  If all are
            external or finished, return true; else false '''
        #+
        # Arguments:	array of [real-path, [array of uuid of transfer requests]]
        # Returns:	Boolean
        # Side Effects:	multiple accesses to LTA REST server
        #		Print if error
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
        for tuuid in trUUID:
            transferRequestData = requests.get('https://lta.icecube.aq/TransferRequests/' + tuuid, auth=self.bearer)
            bundleRequest = requests.get('https://lta.icecube.aq/Bundles?request=' + tuuid, auth=self.bearer)
            trbundle = bundleRequest.json()['results']
            if len(trbundle) <= 0:
                print('AreTransfersComplete: TransferRequest has no bundles', transferRequestData.text)
                continue
            for uu in trbundle:
                bundleuuid.append(uu)
            #
        for uu in bundleuuid:
            bundleStatus = requests.get('https://lta.icecube.aq/Bundles/' + uu, auth=self.bearer)
            stat = bundleStatus.json()['status']
            if stat not in acceptable:
                return False
        return True
    #
    def DeleteDir(self, realDir):
        ''' Delete the contents of the specified directory '''
        #+
        # Arguments:	real directory with contents to be deleted
        #			assumed to have no subdirectories
        # Returns:		True if no problems, False if problems
        # Side Effects:	contents of the directory deleted
        #			Print if error
        # Relies on:	Nothing
        #-
        try:
            list_o_files = glob.glob(realDir + '/*')
        except:
            print('DeleteDir failed to glob', realDir)
            return False
        try:
            for donefile in list_o_files:
                #os.remove(donefile)
                print(donefile)
            return True
        except:
            print('DeleteDir: failed to delete some files from', realDir)
        return False
    #
    def FindAndDelete(self):
        ''' Main routine to drive the deletion of raw files '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Deletes files if appropriate
        #		Lots of LTA REST server accesses
        #		Access my REST server
        #		Print errors
        # Relies on:	GetFullDirsDone
        #		GetAllTransfers
        #		AreTransfersComplete
        #		DeleteDir
        #-
        directoryPairs = self.GetFullDirsDone()
        if len(directoryPairs) <= 0:
            return
        transferRows = self.GetAllTransfer(directoryPairs)
        if len(transferRows) <= 0:
            return
        for transfer in transferRows:
            if self.AreTransfersComplete(transfer):
                ok = self.DeleteDir(transfer[0])
                if not ok:
                    print('FindAndDelete failed in deleting', transfer[0])
                    return	# Do not try to continue

if __name__ == '__main__':
    app = AutoFiles()
    app.FindAndDelete()
