import os
import sys
#import glob
import json
import requests

class BearerAuth(requests.auth.AuthBase):
    ''' Utility class for using the token with requests package '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

class BunCheck():
    ''' Class to handle dumped file deletion '''
    def __init__(self, name='service-token', donename='DONELIST', quarfile='QUARANTINE.INFO', histfile='statuses.json', reqfile='DONEREQUEST.LIST'):
        ''' __init__ for BunCheck; load LTA token '''
        #+
        # Arguments:	optional name of service token file; default service-token
        # Returns:	Nothing
        # Side Effects: Subroutine reads a file
        # Relies on:	getLTAToken
        #		ReadConfig
        #-
        token = self.getLTAToken(name)
        self.bearer = BearerAuth(token)
        self.apriori = ['PFRaw', 'PFDST', 'pDAQ-2ndBld']
        self.bstati = {}
        for status in ['specified', 'created', 'staged', 'transferring', 'taping', 'verifying', 'completed', 'detached', 'source-deleted', 'deleted', 'finished']:
            self.bstati[status] = 0
        # This doesn't look promising by itself, with only the LTA server's info
        self.rstati = {}
        for status in ['completed', 'deprecated', 'processing', 'quarantined']:
            self.rstati[status] = 0
        self.histfile = histfile
        self.quarfile = quarfile
        self.quarinfo = []
        # Read in done bundle info
        self.doneuuid = []
        self.donename = donename
        try:
            fhandle = open(donename, 'r')
        except:
            print('BunCheck:__init__ failed to open', donename)
            sys.exit(1)
        try:
            buncholines = fhandle.readlines()
            fhandle.close()
            for line in buncholines:
                self.doneuuid.append(line.rstrip())
        except:
            print('BunCheck:__init__ failed to read', donename)
            sys.exit(1)
        # Read in finished requests list
        self.donerequuid = []
        self.reqfile = reqfile
        try:
            fhandle = open(reqfile, 'r')
        except:
            print('BunCheck:__init__ failed to open request file', reqfile)
            sys.exit(1)
        try:
            buncholines = fhandle.readlines()
            fhandle.close()
            for line in buncholines:
                self.donerequuid.append(line.rstrip())
        except:
            print('BunCheck:__init__ failed to read request file', reqfile)
            sys.exit(1)
        # Dumper info:  abandoned filesdeleted finished LTArequest neverdelete processing unclaimed
        # From FullDirectory
        # Only unclaimed, processing, and LTArequest are significant in looking for bottlenecks.
        # This DB is keyed by idealName (of the directory), so I need to preserve the TransferRequest uuid and path too
        # The bundles should preserve the uuid and transfer request--I can key back to the transfer request for the path
        # LTArequest doesn't seem to be kept as up-to-date as it ought.  However, modern versions include the 
        # transfer request, and I can cross-check that to see what state that is in the LTA.  Or I can try to bolt on
        # some updating code somewhere, which might be simpler to produce and maintain.
        #

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
        except Exception as e:
            print('getLTAToken failed to read from', tokenfilename, e)
            sys.exit(1)
    #
    def WriteNewDone(self):
        ''' Write out a new done list, overwriting the old '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	writes a new version of the donefile
        # Relies on:	Nothing
        #-
        self.doneuuid.sort()
        try:
            fhandle = open(self.donename, 'w')
        except:
            print('BunCheck:WriteNewDone failed to open for write', self.donename)
            sys.exit(2)
        try:
            for uuid in self.doneuuid:
                fhandle.write(uuid + '\n')
            fhandle.close()
        except Exception as e:
            print('BunCheck:WriteNewDone failed in writing', self.donename, e)
            sys.exit(2)
    #
    def GetBundleLs(self):
        ''' Find the new bundles not known to be done yet '''
        #+
        # Arguments:	None
        # Returns:	array of uuid
        # Side Effects:	Call to REST server
        # Relies on:	REST server
        #-
        bunlist = requests.get('https://lta.icecube.aq/Bundles?query', auth=self.bearer)
        thisb = bunlist.json()['results']
        newuuid = []
        for js in thisb:
            if js not in self.doneuuid:
                newuuid.append(js)
        return newuuid
    #
    def GetBundleInfo(self, uuid):
        ''' Load a dictionary with status information '''
        #+
        # Arguments:	uuid of bundle
        # Returns:	Nothing
        # Side Effects:	Call to REST server
        # Relies on:	REST server
        #-
        donetypes = ['finished', 'deprecated', 'abandoned', 'external']
        bunlist = requests.get('https://lta.icecube.aq/Bundles/' + str(uuid), auth=self.bearer)
        thisb = bunlist.json()
        status = thisb['status']
        # If this bundle is done, add it to the donelist that we'll write out later
        if status in donetypes:
            self.doneuuid.append(thisb['uuid'])
            return
        # Special case.  deleted may not transition to finished if the files are NOT TO BE DELETED
        if status == 'deleted':
            dones = True
            for statype in self.apriori:
                if statype in thisb['path']:
                    dones = False
                    break
            if dones:
                self.doneuuid.append(thisb['uuid'])
                return
        # Load info for quarantined files.  Write these out at the end
        if status == 'quarantined':
            if thisb['claimed'] == 'false':
                claimant = 'unclaimed'
            else:
                claimant = thisb['claimant']
            self.quarinfo.append([uuid, claimant])
            return
        # Histogram bait.
        self.bstati[status] = self.bstati[status] + 1
    #
    def WriteEvent(self):
        ''' Write out the histogram bait, the quarantine info, and the new donelist '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Writes out 3 files (append, append, re-write)
        # Relies on:	WriteNewDone (re-writes the donefile)
        #-
        self.WriteNewDone()
        #
        # append the quarantine info, if any
        if len(self.quarinfo) > 0:
            try:
                fhandle = open(self.quarfile, 'a')
            except:
                print('BunCheck:WriteEvent failed to open quarinfo file', self.quarfile)
                sys.exit(3)
            try:
                for pair in self.quarinfo:
                    fhandle.write(pair[0] + ' ' + pair[1] + '\n')
                fhandle.close()
            except Exception as e:
                print('BunCheck:WriteEvent failed to write to quarinfo file', self.quarfile, e)
                sys.exit(3)
        #
        # append the json histogram bait
        try:
            outfile = open(self.histfile, 'a')
            outfile.write(',')
            json.dump(self.bstati, outfile)
            outfile.write('\n')
            outfile.close()
            #with open(self.histfile, 'a') as outfile:
            #    json.dump(self.bstati, outfile)
        except Exception as e:
            print('BunCheck:WriteEvent failed to write to the histo file', self.histfile, e)
            sys.exit(3)
        #
    #
    def DriveReadIn(self):
        ''' Run the input from the REST server '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Reads from REST server
        # Relies on:	GetBundleLs
        #		GetBundleInfo
        #-
        array = self.GetBundleLs()
        if len(array) <= 0:
            return
        for uuid in array:
            self.GetBundleInfo(uuid)
    #
    ####
    #
    #
    #
    #
    #

if __name__ == '__main__':
    app = BunCheck()
    app.DriveReadIn()
    app.WriteEvent()
