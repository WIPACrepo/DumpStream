import os
import sys
import json
import requests
import Utils as U

DEBUG_JNB = False
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


class YearStatus():
    ''' Display the status of the dump, store, and deletion progress for the given year '''
    #
    def __init__(self, myyear=2019, name='service-token'):
        self.YEAR = str(myyear)
        self.tokenfilename = name
        self.getLTAToken(name)
        #
        self.PREFIX = '/data/exp/IceCube/'
        self.SUFFIX = ['/unbiased/PFRaw/', '/unbiased/PFDST/', '/internal-system/pDAQ-2ndBld']
        self.MONINFO = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        self.TAGLIST = []
        for month in range(12):
            for day in range(self.MONINFO[month]):
                tag = self.gettag(month, day)
                if tag != '':
                    self.TAGLIST.append(tag)
                    continue
        self.TRBLOB = self.GetLTATransfers()
        if os.path.isfile('/bin/du'):
            self.EXECDU = '/bin/du'
        else:
            self.EXECDU = '/usr/bin/du'
        if os.path.isfile('/bin/df'):
            self.EXECDF = '/bin/df'
        else:
            self.EXECDF = '/usr/bin/df'
        self.FS_AVAIL = self.isDataExpPresent()
    #
    def getLTAToken(self, name):
        ''' Read the LTA REST server token from file "name"; return the same '''
        try:
            tf = open(name, 'r')
            self.token = tf.readline()
            self.bearer = BearerAuth(self.token)
            tf.close()
        except:
            print('getLTAToken failed to read from', name)
            sys.exit(1)
    #
    def isDataExpPresent(self):
        ''' Are the /data/exp and /mnt/lfs7 filesystems available? '''
        #+
        # Arguments:	None
        # Returns:	boolean; true if both are present
        # Side Effects:	none
        # Relies on:	Utils.getoutputerrorsimplecommand
        #-
        cmd = [self.EXECDF, '/data/exp']
        _, _, code = U.getoutputerrorsimplecommand(cmd, 1)
        if code != 0:
            return False
        cmd = [self.EXECDF, '/mnt/lfs7']
        _, _, code = U.getoutputerrorsimplecommand(cmd, 1)
        if code != 0:
            return False
        return True
    #
    def gettag(self, mmonth, mday):
        ''' Return a string for the given month and day.  Trivial except for Feb '''
        #+
        # Arguments:	int month (0-11) and day (0-x)
        # Returns:	string of the form 0327 = mmdd
        # Side Effects:	None
        # Relies on:	Nothing
        #-
        try:
            month = int(mmonth)
            day = int(mday)
        except:
            print('YearStatus:gettag  Wants ints!', mmonth, mday)
            sys.exit(1)
        if month < 0 or month > 11 or day < 0:
            return ''
        if day >= self.MONINFO[month]:
            return ''
        if month != 1:
            return "%02d%02d" % (month+1, day+1)
        if int(int(self.YEAR)/4)*4 == self.YEAR:
            return "%02d%02d" % (month+1, day+1)
        if day >= self.MONINFO[month] - 1:
            return ''
        return "%02d%02d" % (month+1, day+1)
    #
    def GetYearInfo(self, dtype):
        ''' Get the info from FullDirectory for the given type and year '''
        #+
        # Arguments:	int type (0-2) of data
        # Returns:	array of info from query, sorted by date
        # Side Effects:	REST server query
        # Relies on:	Utils.mangle, Utils.unNone, Utils.singletodouble
        #-
        # Sanity
        if not isinstance(dtype, int):
            print('YearStatus:GetYearInfo got a bad data type, should be int', dtype)
            sys.exit(2)
        location = {}
        location['likeIdeal'] = self.PREFIX + self.YEAR + self.SUFFIX[dtype]
        query = U.curltargethost + '/directory/info/' + U.mangle(json.dumps(location))
        answers = requests.get(query)
        # PROBLEM:  the answers include unquoted None from the database.
        bigtext = U.unNone(U.singletodouble(answers.text))
        myjanswer = json.loads(bigtext)
        returnarray = []
        for j in myjanswer:
            returnarray.append([j['idealName'].split('/')[-1], j['idealName'], j['dirkey'], j['status'], 0])
        returnarray.sort()
        return returnarray
    #
    def PrintStatus(self, dtype=0):
        ''' Write out the status for each day in the year '''
        #+
        # Arguments:	dtype of data to write
        # Returns:	list of information about the date
        # Side Effects: None, see calling routine
        # Relies on:	GetYearInfo
        #-
        # Sanity
        if not isinstance(dtype, int):
            print('YearStatus:PrintStatus got a bad data type, should be int', dtype)
            sys.exit(2)
        knownstuff = self.GetYearInfo(dtype)
        prefix = self.PREFIX + self.YEAR + self.SUFFIX[dtype]
        print('======== ' + prefix + ' =========')
        knownyet = {}
        for tag in self.TAGLIST:
            knownyet[tag] = []
            for knowndir in knownstuff:
                if tag == knowndir[0]:
                    knownyet[tag] = knowndir
                    break
        # When to stop printing
        lasttag = '1332'
        for tag in sorted(self.TAGLIST, reverse=True):
            if len(knownyet[tag]) != 0:
                break
            lasttag = tag
        # Do the printing
        for tag in self.TAGLIST:
            if tag == lasttag:
                break
            ltastatus = self.RetrieveBundleInfo(prefix + tag)
            # Check the directory?
            if self.FS_AVAIL:
                cmd = [self.EXECDF, '-s', prefix + tag]
                canswer, _, _ = U.getoutputerrorsimplecommand(cmd, 1)
                try:
                    dresult = str(canswer.split('\t')[0])
                except:
                    dresult = 'NotAvail'
            #
            else:
                dresult = 'NotAvail'
            if len(knownyet[tag]) == 0:
                dumpstatus = '-'
            else:
                dumpstatus = knownyet[tag][3]
            print(tag, dumpstatus, ltastatus, dresult)
    #
    def GetLTATransfers(self):
        ''' Get the block of LTA transfer requests '''
        #+
        # Arguments:    None
        # Returns:      json of LTA transfer request info
        # Side Effects: Calls LTA REST server
        # Relies on:    LTA REST server
        #-
        # I may want to cherrypick info, so I'm putting this in its own routine
        #answers = requests.get('https://lta.icecube.aq/Bundles?request=' + 'dcc26d08185011ea899f12aa67a59482', auth=self.bearer)
        #print(answers.text)
        answers = requests.get('https://lta.icecube.aq/TransferRequests', auth=self.bearer)
        return answers.json()['results']
    #
    def RetrieveBundleInfo(self, directory):
        ''' Get status information for the given directory from LTA '''
        #+
        # Arguments:    directory = /data/exp/etc... string
        # Returns:      status of the bundles in this directory
        # Side Effects: Calls LTA REST server
        # Relies on:    LTA REST server
        #-
        # Parse through the self.TRBLOB for this directory path
        # Note that there may be some directories that are /mnt/lfs7/exp...,
        #  so I need to look for part of the directory name
        acceptable = ['external', 'finished', 'deleted', 'deprecated']
        dsplit = directory.split('/exp/')
        if len(dsplit) != 2:
            print('YearStatus:RetrieveBundleInfo does not know what to do with non-exp', directory)
            sys.exit(4)
        foundjsons = []
        for tblob in self.TRBLOB:
            if dsplit[1] in tblob['path']:
                foundjsons.append(tblob)
        if len(foundjsons) == 0:
            return '-'
        # If any request was completed, we're done with this one
        for tblob in foundjsons:
            if tblob['status'] == 'completed':
                if DEBUG_JNB:
                    answerx = requests.get('https://lta.icecube.aq/Bundles?request=' + tblob['uuid'], auth=self.bearer)
                    print('alldone', len(foundjsons), directory, answerx.text)
                return 'completed'
        # Get bundle uuid's associated with transfer requests for this directory
        bundleuuid = []
        for tblob in foundjsons:
            truuid = tblob['uuid']
            answer = requests.get('https://lta.icecube.aq/Bundles?request=' + truuid, auth=self.bearer)
            ansj = answer.json()['results']
            thisrequest = True
            if len(ansj) <= 0:
                thisrequest = False
            for uuid in ansj:
                if DEBUG_JNB:
                    print('bundle', uuid)
                bundleuuid.append(uuid)
                answer = requests.get('https://lta.icecube.aq/Bundles/' + uuid, auth=self.bearer)
                status = answer.json()['status']
                #print('xx', status)
                if status not in acceptable:
                    thisrequest = False
            if thisrequest:
                return 'completed'	# Any set of bundles will do, but not a mix from sets
        if len(bundleuuid) == 0:
            return 'inconsistent'
        #
        return 'processing'


if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            year = int(sys.argv[1])
            app = YearStatus(year)
        except:
            print('Bad year argument', sys.argv[1])
    else:
        app = YearStatus() 
    app.PrintStatus(0)
    #ltaj = app.GetLTATransfers()
    #for x in ltaj:
    #    print(x)
    app.PrintStatus(1)
    app.PrintStatus(2)
    sys.exit(0)
