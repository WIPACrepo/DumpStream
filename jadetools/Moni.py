'''
   Collect tools for monitoring
   LTA modules' status
   Bundle status
   Dump system status
'''

import sys
from datetime import datetime
from time import mktime, strptime
import json
import requests
import Utils as U

class BearerAuth(requests.auth.AuthBase):
    ''' Utility for token handling for LTA '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

class MoniLTA():
    ''' Retrieve status of various LTA modules
        determine their antiquity, and see if
        something has not run recently '''
    def __init__(self, name='service-token', configname='Moni.json'):
        #+
        # Arguments:	optional name of service token file; default service-token
        #		optional name of configuration file; default Moni.json
        # Returns:	Nothing
        # Side Effects:	reads 2 files
        # Relies on:	GetLTAToken
        #		ReadConfig
        #-
        self.token = self.GetLTAToken(name)
        self.bearer = BearerAuth(self.token)
        self.ReadConfig(configname)
        self.modules = ['picker', 'bundler', 'nersc_mover', 'nersc_verifier', 
                        'site_move_verifier', 'replicator', 'deleter',  
                        'rucio_detacher', 'rucio_stager', 'transfer_request_finisher']
        # 'health' isn't needed, but is a possibility 
    #
    def GetLTAToken(self, tokenfilename):
        ''' Read the LTA REST server token from file "tokenfilename"; set it for the class '''
        #+
        # Arguments:    token file name string
        # Returns:      token
        # Side Effects: reads a file
        # Relies on:    file with token
        #-
        try:
            tf = open(tokenfilename, 'r')
            token = tf.readline()
            tf.close()
            return token
        except:
            print('GetLTAToken failed to read from', tokenfilename)
            sys.exit(1)
    #
    def ReadConfig(self, configfilename):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        #+
        # Arguments:    configuration file name
        # Returns:      Nothing
        # Side Effects: reads a file, sets internal json configuration
        # Relies on:    file exists, has json configuration
        #-
        try:
            with open(configfilename) as f:
                self.config = json.load(f)
        except:
            print('ReadConfig failed to read', configfilename)
            sys.exit(2)
    #
    def ReadBlob(self, sub_detail):
        ''' Read the last work end timestamp from the json, return antiquity in seconds '''
        #+
        # Arguments:	chunk of json with time info
        # Returns:	seconds since the timestamp
        # Side Effects:	None
        # Relies on:	Nothing
        #-
        try:
            donetime = sub_detail['last_work_end_timestamp']
        except:
            print('ReadBlog failed to get time info', sub_detail)
            sys.exit(3)
        rightnow = datetime.utcnow()
        bulktime = donetime.split('.')
        stripped = strptime(bulktime[0], "%Y-%m-%dT%H:%M:%S")
        rightthen = datetime.fromtimestamp(mktime(stripped))
        diff = rightnow - rightthen
        seconds = diff.days*86400 + diff.seconds + diff.microseconds/1000000
        return seconds
    #
    def CheckComponent(self, ltaModule, local=''):
        ''' Find the most recent execution time for the module of the given type '''
        #+
        # Arguments:	LTA module name
        #		The value of the local name, if there are both a remote
        #		 and local class of module.
        # Returns:	Nothing for now
        # Side Effects:	prints the LTA module information
        # Relies on:	ReadBlob
        #		LTA REST server up
        #-
        module_info = requests.get(self.config['LTA_REST_URL'] + 'status/' + ltaModule, auth=self.bearer)
        if module_info.status_code != 200:
            print(ltaModule, module_info.status_code)
            return []
        details = module_info.json()
        #
        oldest_remote = 999999999
        if local != '':
            oldest_local = 999999999
        for blob in details:
            antiquity = self.ReadBlob(details[blob])
            if local != '':
                if local in blob:
                    if oldest_local > antiquity:
                        oldest_local = antiquity
                else:
                    if oldest_remote > antiquity:
                        oldest_remote = antiquity
            else:
                if oldest_remote > antiquity:
                    oldest_remote = antiquity
        if local != '':
            return [oldest_remote, oldest_local]
        return [oldest_remote]
    #
    def AllComponents(self):
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	None
        # Relies on:	CheckComponent
        #-
        problem_list = []
        for module in self.modules:
            if module == 'deleter':
                results = self.CheckComponent(module, 'lta-vm-1')
            else:
                results = self.CheckComponent(module)
            if len(results) == 0:
                problem_list.append(module)
                continue
            if len(results) == 1:
                if results[0] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module)
            else:
                if results[0] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module + ' NERSC')
                if results[1] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module + ' local')
        return problem_list

if __name__ == '__main__':
    allc = MoniLTA()
    result = allc.AllComponents()
    if len(result) > 0:
        print(result)
