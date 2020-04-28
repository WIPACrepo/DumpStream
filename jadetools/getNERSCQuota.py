import os
import sys
import json
import requests
import Utils as U

class BearerAuth(requests.auth.AuthBase):
    ''' Translate the LTA REST server token into something useful.
        This relies on the "requests" package
        This initialzes with a string token, and on call returns
        a Bearer token for the requests call to use '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

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
class checkNERSC():
    ''' Encapsulate the NERSC quota check code '''
    def __init__(self, config, name='service-token'):
        self.getLTAToken(name)
        self.config = config
    #
    def __call__(self, filesystem):
        _, used, avail = self.checkQuotaNERSC()
        if used < 0 or avail < 0:
            return False
        if used > self.config['FRACTION_NERSC_QUOTA'] * avail:
            return False
        return True
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
        ''' Retrieve the NERSC cscratch1 space usage and quota and most recent time '''
        r3 = requests.get('https://lta.icecube.aq/status/site_move_verifier', auth=BearerAuth(self.token))
        if r3.status_code != 200:
            print('checkQuotaNERSC: site_move_verifier check', r3.status_code)
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


class checkDu():
    ''' Encapsulate the du checks '''
    def __init__(self, config):
        if os.path.isfile('/bin/du'):
            self.exec = '/bin/du'
        else:
            self.exec = '/usr/bin/du'
        self.confg = config
    #
    def FetchDuPercent(self, target):
        ''' Execute a "du" and parse the output for the % used
            Warning.  du is not in the same location on different OS
            versions.  For example, we have /bin/du on jade03, and
            /usr/bin/du on the cluster nodes.
            The arguments are the target directory/filesystem, and
            an optional location other than /bin/du if you want to run
            on something other than jade03  '''
        cmd = [self.exec, target]
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 1)
        if code != 0 or len(error) > 2:
            print('FetchDuPercent failed', error, answer, code)
            return 200
        www = answer.split('%')
        if len(www) != 3:
            print('FetchDuPercent got more than one answer', answer)
            return 200
        www = answer.split()        # Ignore lines; assumes first answer is correct
        for wword in www:
            if '%' in wword and 'Use' not in wword:
                wx = wword.split('%')
                try:
                    return float(wx[0])
                except:
                    print('FetchDuPercent could not make a number out of', wx[0])
                    return 200
        return 200
    
#####
#
def readConfig(fileName='Dump.json'):
    '''  Read the configuration -- which filesystems and what limits
        from the specified file '''
    try:
        with open(fileName) as f:
            data = json.load(f)
        return data
    except:
        print('readConfig failed to read', fileName)
        return None
 
token = getLTAToken('service-token')
    
time, size, maximum = checkQuotaNERSC(token)
    
percentlfs7 = FetchDuPercent('/mnt/lfs7/jade-lta')
