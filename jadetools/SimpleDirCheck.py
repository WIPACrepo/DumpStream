# SimpleDirCheck.py
'''
	Given a directory, find out if the files within it have been ingested
        into the FileCatalog.
        Check the files on disk, then look for each one.
        Returns "OK" or "NEEDS WORK number-cataloged total-expected"
        Interactive use expected
'''
import os
import sys
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
class SimpleDirCheck():
    ''' Class to check directory ingest status '''
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
    #
    def CompareDirectoryToArchive(self, directory):
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
        #		Prints an answer for the user
        # Relies on:	filesystem up and directory accessible
        #		FileCatalog up and its REST server
        #		directory name is of the form /data/exp/ETC
        cmd = [self.execls, directory]
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 2)
        if code != 0 or len(error) > 2:
            print('CompareDirectoryToArchive failed to do a ls on', directory)
            return 1
        foundFiles = answer.splitlines()
        #
        # Now fetch info from the FileCatalog--the files need to match
        # If I'm missing files in the warehouse, that may not matter,
        # what's a problem is when there are files that aren't registered
        # here or at NERSC
        dwords = directory.split(self.dirsplit)
        if len(dwords) != 2:
            print('CompareDirectoryToArchive could not parse the exp in the directory name', directory)
            return 2
        #
        directoryFrag = dwords[1]
        #
        query_dictw = {"locations.archive": {"$eq": True,}, "locations.site": {"$eq": "WIPAC"}, "logical_name": {"$regex": directoryFrag}}
        query_jsonw = json.dumps(query_dictw)
        overallw = self.config['FILE_CATALOG_REST_URL'] + f'/api/files?query={query_jsonw}'
        rw = requests.get(overallw, auth=self.bearer)
        #
        # Try to unpack the info.
        try:
            fileWIPAC = rw.json()['files']
        except:
            print('CompareDirectoryToArchive failed to unpack WIPAC-based files')
            return 3
        #
        if len(foundFiles) == len(fileWIPAC):
            print('OK')
            return 0
        print('NEEDS WORK', len(fileWIPAC), len(foundFiles))
        return 4
        #
    #

if __name__ == '__main__':
    app = SimpleDirCheck()
    if len(sys.argv) <= 1:
        print('Needs a directory argument')
        sys.exit(1)
    value = app.CompareDirectoryToArchive(str(sys.argv[1]))
