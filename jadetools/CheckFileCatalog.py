import os
import sys
import json
import requests
import Utils as U

#####
#
class BearerAuth(requests.auth.AuthBase):
    ''' Translate the LTA REST server token into something useful.
        This relies on the "requests" package
        This initialzes with a string token, and on call returns
        a Bearer token for the requests call to use '''
    def __init__(self, stoken):
        self.token = stoken
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r



#####
#
class CheckFileCatalog():
    ''' Encapsulate the FileCatalog code '''
    #
    def __init__(self, name='service-token', configname='Dump.json'):
    #+
    # Arguments:   optional name of service token file; default service-token
    #               optional name of configuration file; default Dump.json
    # Returns:      Nothing
    # Side Effects: Subroutine reads a file
    # Relies on:    getLTAToken
    #	            ReadConfig
    #               U.GiveTarget
    #-
        self.tokenfilename = name
        self.configfilename = configname
        self.getLTAToken(name)
        self.config = self.ReadConfig()
        if os.path.isfile('/bin/ls'):
            self.execls = '/bin/ls'
        else:
            self.execls = '/usr/bin/ls'
        self.apriori = ['PFRaw', 'PFDST', 'pDAQ-2ndBld']
        self.dumptargetdir = U.GiveTarget()
    ##
    def getLTAToken(self, name):
        ''' Read the LTA REST server token from file "name"; return the same '''
        #+
        # Arguments:    token file name string
        # Returns:      token
        # Side Effects: reads a file, if possible
        # Relies on:    file with token
        #-
        try:
            tf = open(name, 'r')
            self.token = tf.readline()
            tf.close()
        except:
            print('getLTAToken failed to read from', name)
            sys.exit(1)
    #
    def ReadConfig(self):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        #+
        # Arguments:    configuration file name
        # Returns:      json with configuration from file
        # Side Effects: reads a file
        # Relies on:    file exists, has json configuration
        #-
        try:
            with open(self.configfilename) as f:
                data = json.load(f)
            return data
        except:
            print('ReadConfig failed to read', self.configfilename)
            return None
    ###
    def compareDirectoryToArchive(self, directory):
        ''' Does the listing of the directory include files not found
            in the remotely (NERSC) archived
            list?  If so, return a non-zero code
            If everything is in the archive lists, return 0 '''
        #+
        # Arguments:	directory name
        # Returns:	0 if everything present is archived, else codes
        # Side Effects:	query file-catalog
        # Relies on:	U.getoutputerrorsimplecommand with ls
        #		file-catalog OK
        #-
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
        dwords = directory.split('/exp/')
        if len(dwords) != 2:
            print('compareDirectoryToArchive could not parse the exp in the directory name', directory)
            return 2
        #
        directoryFrag = '^/data/exp/' + dwords[1]
        #
        #query_dictn = {"locations.archive": {"$eq": True,}, "locations.site": {"$eq": "NERSC"}, "logical_name": {"$regex": directoryFrag}}
        query_dictn = {"locations.site": {"$eq": "NERSC"}, "logical_name": {"$regex": directoryFrag}}
        query_jsonn = json.dumps(query_dictn)
        overalln = self.config['FILE_CATALOG_REST_URL'] + f'/api/files?query={query_jsonn}'
        rn = requests.get(overalln, auth=BearerAuth(self.token))
        # Try to unpack the info.
        try:
            fileNERSC = rn.json()['files']
        except:
            print('compareDirectoryToArchive failed to unpack NERSC-based files')
            return 4
        if len(fileNERSC) <= 0:
            return 5
        #
        ner = []
        for z in fileNERSC:
            ner.append(z['logical_name'])
        # logical_name
        for ff in foundFiles:
            foundIt = False
            for arch in ner:
                if ff in arch:
                    foundIt = True
                    break
            if not foundIt:
                return 7
        return 0
    #
    def GetFullDirsNotEmptied(self):
        ''' Return an array of FullDirectories where toLTA=2, only of expected types '''
        #+
        # Arguments:    None
        # NOT USED
        # Returns:      array of idealNames from FullDirectories
        #               where toLTA=2 and directory is one of the expected types
        # Side Effects: query of my REST server
        # Relies on:    Utils.mangle
        #               my REST server is up
        #-
        bigreq = 'http://archivecontrol.wipac.wisc.edu/dumping/handedoffdir/' + U.mangle('/exp/ 2')
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
    #
    def VetNotOfficialTree(self, logicalDirectoryName):
        ''' Check that this directory is
             1) Really a directory
             2) Has a real path that is part of the dump target area,
               not the true data warehouse.  It should be linked to
               from the warehouse 
            returns True if OK; False if something not OK '''
        #+
        # Arguments:    directory path name
        # NOT USED
        # Returns:      boolean
        # Side Effects: os filesystem metadata retrieved
        # Relies on:    fileystem is up
        #-
        #
        if not os.path.isdir(logicalDirectoryName):
            return False
        if self.dumptargetdir not in os.path.realpath(logicalDirectoryName):
            return False
        if '/ceph/' in os.path.realpath(logicalDirectoryName):
            return False
        return True

#
####
# main
if __name__ == '__main__':
    checker = CheckFileCatalog()
    successType = checker.compareDirectoryToArchive('/data/exp/IceCube/2018/unbiased/PFRaw/1206')
