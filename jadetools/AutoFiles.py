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
import glob
import json
import requests
import Utils as U
import CheckFileCatalog as LC

JNB_DEBUG = False
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
    def __init__(self, name='service-token', configname='Dump.json'):
        ''' __init__ for AutoFiles; load LTA token '''
        #+
        # Arguments:	optional name of service token file; default service-token
        #		optional name of configuration file; default Dump.json
        # Returns:	Nothing
        # Side Effects: Subroutine reads a file
        # Relies on:	getLTAToken
        #		ReadConfig
        #-
        token = self.getLTAToken(name)
        self.bearer = BearerAuth(token)
        self.config = self.ReadConfig(configname)
        if os.path.isfile('/bin/ls'):
            self.execls = '/bin/ls'
        else:
            self.execls = '/usr/bin/ls'
        self.apriori = ['PFRaw', 'PFDST', 'pDAQ-2ndBld']
        self.dumptargetdir = U.GiveTarget()
        self.dirsplit = '/exp/'
        self.checker = LC.CheckFileCatalog()
    #
    def getLTAToken(self, tokenfilename):
        ''' Read the LTA REST server token from file "tokenfilename"; set it for the class '''
        #+
        # Arguments:	token file name string
        # Returns:	token
        # Side Effects:	reads a file, if possible
        # Relies on:	file with token
        #-
        try:
            tf = open(tokenfilename, 'r')
            token = tf.readline()
            tf.close()
            return token
        except:
            print('getLTAToken failed to read from', tokenfilename)
            sys.exit(1)
    #
    def ReadConfig(self, configfilename):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        #+
        # Arguments:	configuration file name
        # Returns:	json with configuration from file
        # Side Effects:	reads a file
        # Relies on:	file exists, has json configuration
        #-
        try:
            with open(configfilename) as f:
                data = json.load(f)
            return data
        except:
            print('ReadConfig failed to read', configfilename)
            return None
    #
    def getFullDirsNotEmptied(self):
        ''' Return an array of FullDirectories where toLTA=2, only of expected types '''
        #+
        # Arguments:	None
        # Returns:	array of idealNames from FullDirectories 
        #		where toLTA=2 and directory is one of the expected types
        # Side Effects:	query of my REST server
        # Relies on:	my REST server is up
        #-
        bigreq = 'http://archivecontrol.wipac.wisc.edu/dumping/handedoffdir/' + U.mangle(self.dirsplit + ' 2')
        fulldirs = requests.get(bigreq)
        unjson = eval(fulldirs.text)
        undone = []
        for blob in unjson:
            candidate = blob['idealName']
            for ap in self.apriori:
                if ap in candidate:
                    undone.append(candidate)
                    break
        return undone
    #
    def VetNotOfficialTree(self, logicalDirectoryName):
        ''' Check that this directory is
             1) Really a directory
             2) Has a real path that is part of the dump target area,
               not the true data warehouse.  It should be linked to
               from the warehouse
            returns True if OK; False if something not OK '''
        #+
        # Arguments:	directory path name
        # Returns:	boolean
        # Side Effects:	os filesystem metadata retrieved
        # Relies on:	fileystem is up
        #-
        if not os.path.isdir(logicalDirectoryName):
            return False
        if self.dumptargetdir not in os.path.realpath(logicalDirectoryName):
            return False
        return True
    #
    #
    def compareDirectoryToArchive(self, directory):
        ''' Does the listing of the directory include files not found
            in the locally (WIPAC) archived or remotely (NERSC) archived
            list?  If so, return a non-zero code
            If everything is in the archive lists, return 0 '''
        #+
        # Arguments:	directory name to investigate; ideal name
        #		NOTE:  must be linked to from the warehouse
        #		The real name is not what's wanted
        # Returns:	Integer code 0=OK, others represent different problems
        # Side Effects:	filesystem listing
        #		FileCatalog queries
        # Relies on:	filesystem up and directory accessible
        #		FileCatalog up and its REST server
        #		directory name is of the form /data/exp/ETC
        cmd = [self.execls, directory]
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 2)
        if code != 0 or len(error) > 2:
            print('compareDirectoryToArchive failed to do a ls on', directory)
            return 1
        foundFiles = answer.splitlines()
        #
        # Now fetch info from the FileCatalog--the files need to match
        # If I'm missing files in the warehouse, that may not matter,
        # what's a problem is when there are files that aren't registered
        # here or at NERSC
        dwords = directory.split(self.dirsplit)
        if len(dwords) != 2:
            print('compareDirectoryToArchive could not parse the exp in the directory name', directory)
            return 2
        #
        directoryFrag = dwords[1]
        #
        query_dictw = {"locations.archive": {"$eq": True,}, "locations.site": {"$eq": "WIPAC"}, "logical_name": {"$regex": directoryFrag}}
        query_jsonw = json.dumps(query_dictw)
        overallw = self.config['FILE_CATALOG_REST_URL'] + f'/api/files?query={query_jsonw}'
        rw = requests.get(overallw, auth=self.bearer)
        #
        query_dictn = {"locations.archive": {"$eq": True,}, "locations.site": {"$eq": "NERSC"}, "logical_name": {"$regex": directoryFrag}}
        query_jsonn = json.dumps(query_dictn)
        overalln = self.config['FILE_CATALOG_REST_URL'] + f'/api/files?query={query_jsonn}'
        rn = requests.get(overalln, auth=self.bearer)
        # Try to unpack the info.
        try:
            fileWIPAC = rw.json()['files']
        except:
            print('compareDirectoryToArchive failed to unpack WIPAC-based files')
            return 3
        try:
            fileNERSC = rn.json()['files']
        except:
            print('compareDirectoryToArchive failed to unpack NERSC-based files')
            return 4
        if len(fileWIPAC) <= 0 or len(fileNERSC) <= 0:
            return 5
        #
        wip = []
        ner = []
        for z in fileWIPAC:
            wip.append(z['logical_name'])
        for z in fileNERSC:
            ner.append(z['logical_name'])
        # logical_name
        for ff in foundFiles:
            foundIt = False
            for arch in wip:
                if ff in arch:
                    foundIt = True
                    break
            if not foundIt:
                return 6
            #
            foundIt = False
            for arch in ner:
                if ff in arch:
                    foundIt = True
                    break
            if not foundIt:
                return 7
        return 0
    #
    #
    def GetFullDirsDone(self):
        ''' Get a list of pairs of ideal/real directory names where toLTA=2
            toLTA=2 means handed off to LTA system.  toLTA=3 means cleaned up 
            Skip directories not found in a dump area, for safety '''
        #+
        # Arguments:	None
        # Returns:	List of [idealName, realName] pairs for the toLTA=2 directories
        # Side Effects:	Print if failure (keeps going w/o failed one)
        # Relies on:	MatchIdealToRealDir
        #		My REST server working
        #		Working with /data/exp dumps!
        #-
        # Request everything handed off, and parse it for the handed-off
        # "exp" should get pretty much anything in we want
        combin = U.mangle(self.dirsplit[0:-1]) + ' 2'
        bulkReceive = requests.get(U.curltargethost + 'dumping/handedoffdir/' + combin)
        bulkList = eval(str(bulkReceive.text))
        breturn = []
        if len(bulkList) <= 0:
            return breturn
        breturn = []
        #
        for chunk in bulkList:
            dname = chunk['idealName']
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
            print('AreTransfersComplete: SHOULD NOT HAPPEN')
            return False	# Something is wrong, don't break anything
        # Which bundle states are OK to allow deletion of raw files
        #acceptable = ['external', 'finished', 'deleted', 'source-deleted', 'detached', 'completed', 'deprecated']
        acceptable = ['external', 'finished', 'deleted', 'deprecated']
        bundleuuid = []
        for tuuid in trUUID:
            try:
                transferRequestData = requests.get('https://lta.icecube.aq/TransferRequests/' + tuuid, auth=self.bearer)
                bundleRequest = requests.get('https://lta.icecube.aq/Bundles?request=' + tuuid, auth=self.bearer)
            except:
                print('AreTransfersComplete died when lta.icecube.aq failed to reply', tuuid, len(trUUID))
                sys.exit(1)
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
            if JNB_DEBUG:
                print('dEBUG AreTransfersComplete:', stat, bundleStatus.json()['uuid'], infoRow[0])
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
                if JNB_DEBUG:
                    print('dEBUG DeleteDir pretending to remove', donefile)
                else:
                    os.remove(donefile)
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
                ok = self.checker.compareDirectoryToArchive(transfer[0])
                if not ok:
                    continue
                ok = self.DeleteDir(transfer[0])
                if not ok:
                    print('FindAndDelete failed in deleting', transfer[0])
                    return	# Do not try to continue
                ok = self.ResetStatus(transfer[0])
                if not ok:
                    print('FindAndDelete failed to reset the FullDirectories entry status', transfer[0])
                    return	# Do not try to continue
            else:
                print('FindAndDelete: not all the Transfers are complete in', transfer)
            if JNB_DEBUG:
                print('dEBUG FindAndDelete:  stop here for test A', transfer)

    ##
    def ResetStatus(self, idealDirectory):
        ''' Set toLTA=3 in FullDirectories for directory idealDirectory '''
        #+
        # Arguments:	directory name (string)
        # Returns:	boolean for success/failure
        # Side Effects:	updates my REST server
        # Relies on:	my REST server working
        #-
        if JNB_DEBUG:
            print('ResetStatus', idealDirectory)
            return True
        updurl = 'http://archivecontrol.wipac.wisc.edu/workupdate/' + U.mangle(idealDirectory + ' 3')
        rw = requests.post(updurl)
        if 'FAILURE' in rw.text:
            return False
        return True

if __name__ == '__main__':
    app = AutoFiles()
    app.FindAndDelete()
