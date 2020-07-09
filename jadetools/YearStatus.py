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
    #
    def getLTAToken(self, name):
        ''' Read the LTA REST server token from file "name"; return the same '''
        try:
            tf = open(name, 'r')
            self.token = tf.readline()
            tf.close()
        except:
            print('getLTAToken failed to read from', name)
            sys.exit(1)
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
        print('======== ' + self.PREFIX + self.YEAR + self.SUFFIX[dtype] + ' =========')
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
            if len(knownyet[tag]) == 0:
                print(tag)
            else:
                print(tag, knownyet[tag][3])

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
    app.PrintStatus(1)
    app.PrintStatus(2)
