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
        ''' Drive the readout of each module, record problems '''
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
                    problem_list.append(module + '#' + str(int(results)))
            else:
                if results[0] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module + '-NERSC' + '#' + str(int(results)))
                if results[1] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module + '-local' + '#' + str(int(results)))
        return problem_list
    #
    def WriteStatusFile(self):
        ''' Open the file used for check_mk status 
            Call the various component checkers
            Call the component nagios summarizers
            Close the file
            '''
        #+ 
        # Arguments:    Nothing
        # Returns:      file handle for opened file
        # Side Effects: Opens the flag file
        # Relies on:    Nothing
        #-
        checkmk_file = self.config['CHECKMK_FILE']
        try:
            fhandle = open(checkmk_file, 'w')
        except:
            print('WriteStatusFile failed to open', checkmk_file)
            sys.exit(5)
        # I print out a timestamp to help the helper script figure out if something's wrong
        # and if the last pass was too long ago
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        lta_problem_list = self.AllComponents()
        fhandle.write(self.LTAToString(lta_problem_list) + '\n')
        # TBD
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        dump_problem_list = []
        fhandle.write(self.DumpToString(dump_problem_list) + '\n')
        # TBD
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        fhandle.write(self.BundlesToString() + '\n')
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        fhandle.close()
    #
    #
    def LTAToString(self, problem_list):
        ''' Compose a string summary of problem_list (from LTA system) to a
             network file system file to be read elsewhere '''
        #+
        # Arguments:	list of overdue modules with times
        # Returns:	String for nagios-reader to use
        # Side Effects:	None
        # Relies on:	Nothing
        #-
        if len(problem_list) == 0:
            return '0 LTAModules - OK'
        oldest = 0
        for problem in problem_list:
            try:
                thistime = int(problem.split('#')[1])
            except:
                print('LTAToString format issue', problem)
                sys.exit(6)
            if thistime > oldest:
                oldest = thistime
        #
        return '2 LTAModules oldest:' + str(oldest) + ' CRIT - ' + problem_list
    #
    def DumpToString(self, dump_problem_list):
        ''' compose a string summary of dump_problem_list (from Dump system) to a
             network file system file to be read elsewhere '''
        #+ 
        # Arguments:    list of overdue modules with times and problems
        # Returns:      String for nagios-reader to use
        # Side Effects: None
        # Relies on:    Nothing
        #-
        if len(dump_problem_list) == 0:
            return '0 DumpModules - OK'
        # This is not the final version
        # NOT IMPLEMENTED
        return '2 DumpModules - CRIT - ' + dump_problem_list
    #
    def BundlesToString(self):
        ''' compose a string summary of bundle_summary_dict (from LTA system) to a
             network file system file to be read elsewhere '''
        #+ 
        # Arguments:    None
        # Returns:      String for nagios-reader to use
        # Side Effects: None
        # Relies on:    GetAllActiveBundles
        #		AccumulateBundleStats
        #-
        # This step can easily take over 10 minutes!!
        bundleList = self.GetAllActiveBundles()
        bundle_summary_dict = self.AccumulateBundleStats(bundleList)
        flag = False
        metstring = '|'
        for word in bundle_summary_dict:
            if flag:
                metstring = metstring + ' '
            flag = True
            metstring = metstring + word + '=' + str(bundle_summary_dict[word])
        if bundle_summary_dict['quarantined'] > 0 or bundle_summary_dict['overdue'] > 0:
            outstr = '2 BundleStatus ' +  metstring + ' Crit'
        else:
            outstr = '0 BundleStatus ' + metstring + ' OK'
        return outstr
    #
    def GetAllActiveBundles(self):
        ''' Get the full list of transfer requests from LTA '''
        #+
        # Arguments:	None
        # Returns:	array of [transfer UUID]
        # Side Effects:	reads LTA server
        # Relies on:	LTA REST server working
        #-
        allTransferRequests = requests.get(self.config['LTA_REST_URL'] + 'TransferRequests', auth=self.bearer)
        returnList = []
        for entry in allTransferRequests.json()['results']:
            if entry['dest'] != 'NERSC':
                continue
            status = entry['status']
            if status in ('completed', 'quarantined'):
                continue
            uuid = entry['uuid']
            updateTime = entry['update_timestamp']
            bundleRequest = requests.get(self.config['LTA_REST_URL'] + 'Bundles?request=' + uuid, auth=self.bearer)
            trbundle = bundleRequest.json()['results']
            bunlist = []
            for uu in trbundle:
                bundleStatus = requests.get(self.config['LTA_REST_URL'] + 'Bundles/' + uu, auth=self.bearer)
                bsj = bundleStatus.json()
                stat = bsj['status']
                timestamp = bsj['create_timestamp']
                try:
                    ts2 = bsj['update_timestamp']
                    if ts2 > timestamp:
                        timestamp = ts2
                except:
                    pass
                try:
                    ts2 = bsj['claimed_timestamp']
                    if ts2 > timestamp:
                        timestamp = ts2
                except:
                    pass
                bunlist.append([uu, stat, timestamp])
            returnList.append([uuid, status, updateTime, bunlist])
        return returnList
    #
    def AccumulateBundleStats(self, active_list):
        ''' Parse the list of active transfers to find stats '''
        #+
        # Arguments:	array of [requestUUID, requestStatus, requestUpdateTime, [[bundleUUID, bundleStatus, bundleTime]] ]
        # Returns:	dict of statistics
        # Side Effects:	None
        # Relies on:	Nothing
        #-
        expected_types = ['specified', 'created', 'staged', 'transferring', 'taping',
                          'verifying', 'completed', 'detached', 'source-deleted',
                          'deleted', 'finished', 'external', 'deprecated', 'quarantined', 'overdue']
        do_not_worry = ['external', 'deprecated', 'quarantined']
        #
        dlist = {}
        for ty in expected_types:
            dlist[ty] = 0
        rightnow = datetime.utcnow()
        for request in active_list:
            for bundle in request[3]:
                status = bundle[1]
                if status in do_not_worry:
                    dlist[status] = dlist[status] + 1
                    continue
                stripped = strptime(bundle[2], "%Y-%m-%dT%H:%M:%S")
                rightthen = datetime.fromtimestamp(mktime(stripped))
                diff = rightnow - rightthen
                minutes = diff.days*86400 + diff.seconds
                if minutes > self.config['OVERDUE_SHORT']:
                    dlist['overdue'] = dlist['overdue'] + 1
                    continue
                dlist[status] = dlist[status] + 1
        return dlist

if __name__ == '__main__':
    allc = MoniLTA()
    allc.WriteStatusFile()
