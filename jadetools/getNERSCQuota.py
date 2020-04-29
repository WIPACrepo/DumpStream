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
#####
#
def normalizeAnswer(quotaString):
    ''' Given a string with the info from NERSC quota reply,
        return the value in GiB, no matter the original units
        If there is a problem, the answer is 0 '''
    if len(quotaString) <= 0:
        return 0
    keychar = ['Ki', 'Mi', 'Gi', 'Ti', 'Pi']
    scale = 1/(1024*1024*1024)
    for keyv in keychar:
        scale = scale * 1024
        if keyv in quotaString:
            words = quotaString.split(keyv)
            try:
                iv = float(words[0])
                return scale*iv
            except:
                return 0   # Assume no problems
    return 0  # Assume no problems

#####
#
class CheckExternals():
    ''' Encapsulate the NERSC quota check code '''
    def __init__(self, name='service-token', configname='Dump.json'):
        self.tokenfilename = name
        self.configfilename = configname
        self.getLTAToken(name)
        self.config = self.ReadConfig()
        if os.path.isfile('/bin/du'):
            self.execdu = '/bin/du'
        else:
            self.execdu = '/usr/bin/du'
        if os.path.isfile('/bin/df'):
            self.execdf = '/bin/df'
        else:
            self.execdf = '/usr/bin/df'
    #
    def __call__(self, filesystem):
        return self.go_nogo()
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
    def checkQuotaNERSC(self):
        ''' get the cscratch1 use from NERSC--are we too full? '''
        _, used, avail = self.checkQuotaNERSC()
        if used < 0 or avail < 0:
            return False
        if used > self.config['FRACTION_NERSC_QUOTA'] * avail:
            return False
        return True
    #
    def getQuotaNERSC(self):
        ''' Retrieve the NERSC cscratch1 space usage and quota and most recent time '''
        r3 = requests.get('https://lta.icecube.aq/status/site_move_verifier', auth=BearerAuth(self.token))
        if r3.status_code != 200:
            print('getQuotaNERSC: site_move_verifier check', r3.status_code)
            return '', -1, -1
        details = r3.json()
        latestTime = ''
        latestQuota = ''
        latestUsed = ''
        for blob in details:
            det = details[blob]
            quoti = det['quota']
            ts = det['timestamp']
            for htype in quoti:
                if htype['FILESYSTEM'] == 'cscratch1':
                    if ts > latestTime:
                        latestTime = ts
                        latestUsed = htype['SPACE_USED']
                        latestQuota = htype['SPACE_QUOTA']
        return latestTime, normalizeAnswer(latestUsed), normalizeAnswer(latestQuota)


    def FetchDfPercent(self, target):
        ''' Execute a "df" and parse the output for the % used
            Warning.  df is not in the same location on different OS
            versions.  For example, we have /bin/du on jade03, and
            /usr/bin/du on the cluster nodes.
            The arguments are the target directory/filesystem, and
            an optional location other than /bin/du if you want to run
            on something other than jade03  '''
        cmd = [self.execdf, target]
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 1)
        if code != 0 or len(error) > 2:
            print('FetchDfPercent failed', error, answer, code)
            return False
        www = answer.split('%')
        if len(www) != 3:
            print('FetchDfPercent got more than one answer', answer)
            return False
        www = answer.split()        # Ignore lines; assumes first answer is correct
        for wword in www:
            if '%' in wword and 'Use' not in wword:
                wx = wword.split('%')
                try:
                    flans = float(wx[0])
                    break
                except:
                    print('FetchDfPercent could not make a number out of', wx[0])
                    return False
        if flans > self.config['FRACTION_FS']:
            return False
        return True

    def CheckLocalSpace(self):
        ''' Execute a du on the stage and out areas; should not exceed limit '''
        cmd = [self.execdu, '-h', self.config['STAGE_ROOT'] + '/bundler_out']
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 1)
        if code != 0 or len(error) > 2:
            print('CheckLocalSpace failed', error, answer, code, 'bundler_out')
            return False
        try:
            outsize = int(answer.split()[0])
        except:
            print('CheckLocalSpace failed to get out size', answer)
            return False
        if outsize/1024 > self.config['SIZE_BUFFER_GB']:
            return False
        cmd = [self.execdu, '-h', self.config['STAGE_ROOT'] + '/bundler_stage']
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 1)
        if code != 0 or len(error) > 2:
            print('CheckLocalSpace failed', error, answer, code, 'bundler_stage')
            return False
        try:
            stagesize = int(answer.split()[0])
        except:
            print('CheckLocalSpace failed to get stage size', answer)
            return False
        if (stagesize + outsize)/1024 > self.config['SIZE_ALL_LOCAL_BUNDLE_GB']:
            return False
        return True

    def go_nogo(self):
        ''' Put all the calls in a single place.  Returns False if we are blocked '''
        if not self.FetchDfPercent(self.config['ROOT']):
            return False
        if not self.FetchDfPercent(self.config['STAGE_ROOT']):
            return False
        if not self.CheckLocalSpace():
            return False
        if not self.checkQuotaNERSC():
            return False
        return True

    def ReadConfig(self):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        try:
            with open(self.configfilename) as f:
                data = json.load(f)
            return data
        except:
            print('ReadConfig failed to read', self.configfilename)
            return None
    
#
#####
# main
if __name__ == '__main__':
    app = CheckExternals()
    if app.go_nogo():
        sys.exit(0)	#OK
    sys.exit(1)
 
