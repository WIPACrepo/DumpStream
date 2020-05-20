'''
   Collect tools for monitoring
   LTA modules' status
   Bundle status
   Dump system status
'''
import os
import sys
from datetime import datetime
from time import mktime, strptime
import json
import requests
import Utils as U

DEBUG_JNB = False

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
        if os.path.isfile('/usr/bin/mv'):
            self.execmv = '/usr/bin/mv'
        else:
            self.execmv = '/bin/mv'
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
        # Returns:	List of the age in seconds
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
                    problem_list.append(module + '#' + str(int(results[0])))
            else:
                if results[0] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module + '-NERSC' + '#' + str(int(results[0])))
                if results[1] > self.config['OVERDUE_SHORT']:
                    problem_list.append(module + '-local' + '#' + str(int(results[0])))
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
            fhandle = open(checkmk_file + '.temp', 'w')
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
        fhandle.write(self.GetDumpSystem() + '\n')
        # TBD
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        fhandle.write(self.BundlesToString() + '\n')
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        file_deleter_ok = self.CheckDeleter()
        if file_deleter_ok:
            fhandle.write('0 Interface - OK\n')
        else:
            fhandle.write('1 Interface - Warn taking too long\n')
        fhandle.write('0 LTAM ltamonitor=' + datetime.now().isoformat() + ' OK\n')
        fhandle.close()
        cmd = [self.execmv, checkmk_file + '.temp', checkmk_file]
        goutp, gerro, gcode = U.getoutputerrorsimplecommand(cmd, 1)
        if gcode != 0:
            print('WriteStatusFile error:', goutp, gerro, gcode)
        
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
        return '2 LTAModules oldest:' + str(oldest) + ' CRIT - ' + str(problem_list)
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
        infostring = ''
        if bundle_summary_dict['quarantined'] > 0:
            infostring = infostring + ' ' + str(bundle_summary_dict['quarantined']) + ' Quarantined files'
        if bundle_summary_dict['overdue'] > 0:
            infostring = infostring + ' ' + str(bundle_summary_dict['overdue']) + ' Overdue files'
        if bundle_summary_dict['quarantined'] + bundle_summary_dict['overdue'] > 1:
            outstr = '2 BundleStatus ' +  metstring + ' Crit' + infostring
        if bundle_summary_dict['quarantined'] > 1 or bundle_summary_dict['overdue'] > 0:
            outstr = '1 BundleStatus ' +  metstring + ' Warn' + infostring
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
    #
    def GetDumpSystem(self):
        ''' Fetching Dump system info '''
        #+
        # Arguments:	None
        # Returns:	string for check_mk
        # Side Effects:	access to my REST server
        # Relies on:	REST server up
        #-
        dumpcontrol_get = requests.get(U.curltargethost + '/dumping/state')
        allslots_get = requests.get(U.curltargethost + '/dumping/fullslots')
        fulldir_get = requests.get(U.curltargethost + '/dumping/countready')
        dumpcontrol = eval(dumpcontrol_get.text)
        if DEBUG_JNB:
            print('DumpControl is ', dumpcontrol['status'], ' next up is ', dumpcontrol['nextAction'])
        hhh = eval(allslots_get.text)
        total_slots = len(hhh)
        count_inv = 0
        count_done = 0
        count_dumping = 0
        count_error = 0
        oldest_start_date = '3030-01-01 12:12:12'
        newest_end_date = '1010-01-01 12:12:12'
        #
        for h in hhh:
            if h['status'] == 'Inventoried':
                count_inv = count_inv + 1
            if h['status'] == 'Done':
                count_done = count_done + 1
                end_date = h['dateEnded']
                if end_date > newest_end_date:
                    newest_end_date = end_date
            if h['status'] == 'Dumping':
                count_dumping = count_dumping + 1
                start_date = h['dateBegun']
                if start_date < oldest_start_date:
                    oldest_start_date = start_date
            if h['status'] == 'Error':
                count_error = count_error + 1
        oldest = datetime.strptime(oldest_start_date, "%Y-%m-%d %H:%M:%S")
        newest = datetime.strptime(newest_end_date, "%Y-%m-%d %H:%M:%S")
        rightnow = datetime.now()
        diff = rightnow - oldest
        oldhour = int((diff.days*86400 + diff.seconds) / 3600)
        # if oldhour <0, start date is in the future.
        if oldhour < 0:
            oldhour = 0
        diff = rightnow - newest
        newhour = int((diff.days*86400 + diff.seconds) / 3600)
        if DEBUG_JNB:
            print('Total known slots=', total_slots, ' Done/Dumping/ToDo/Err=', str(count_done) + '/' +
                  str(count_dumping), '/', str(count_inv) + '/' + str(count_error), ' OLDEST start=', oldest_start_date, 
                  ' ', oldhour, ' NEWEST end=', newest_end_date, ' ', newhour)
        string_begin = 'Full Directories: '
        jjj = eval(fulldir_get.text)
        unprocessed = 0
        inprogress = 0
        withlta = 0
        for h in jjj:
            for mtype in h:
                if str(mtype) != 'total' and int(h[mtype]) > 0:
                    string_begin = string_begin + '   ' + str(mtype) + ':' + str(h[mtype])
                    if str(mtype) == 'unstaged':
                        unprocessed = int(h[mtype])
                    if str(mtype) == 'staged':
                        inprogress = int(h[mtype])
                    if str(mtype) == 'done':
                        withlta = int(h[mtype])
        if DEBUG_JNB:
            print(string_begin)
        #
        crit = 0
        errors = ''
        # Are we short of disks to read?
        if count_error > 0 or count_inv < 1:
            #
            if count_error > 0:
                errors = errors + 'Read failure '
                crit = 2
            if count_inv < 1:
                errors = errors + 'Need to load more Pole disks '
                crit = 1
        #
        # Are we not in dumping mode?
        if dumpcontrol['status'] != 'Dumping' or dumpcontrol['nextAction'] != 'Dump':
            crit = 1
            errors = errors + 'Not dumping yet '
        # Are we overdue on a dump cycle?
        if dumpcontrol['status'] == 'Dumping':
            if oldhour > newhour and oldhour > self.config['OVERDUE_LONG']/3600:
                crit = 2
                errors = errors + 'Dump taking too long ' + str(oldhour) + ' hours '
        #
        if crit == 0:
            mess = 'OK'
        if crit == 1:
            mess = 'Warn'
        if crit == 2:
            mess = 'Crit'
        dump_info_string = ('readyDirs=' + str(unprocessed) + ' interfacing=' 
                            + str(inprogress) + ' withLTA=' + str(withlta) + ' ')
        return str(crit) + ' DumpModules | ' + dump_info_string + ' ' + mess + ' ' + errors
    #
    def CheckDeleter(self):
        ''' Is it active?
             No:  probably OK, unless backlog [figured elsewhere]
             Yes:  Is it recent?  2 hours should be more than enough time!!
                  Yes:  probably OK
                  No:  problem '''
        #+
        # Arguments:	None
	# Returns:	boolean Ok/not
        # Side Effects:	access REST server
        # Relies on:	REST server up
        answers = requests.post(U.targetgluedeleter + 'QUERY')
        if len(answers.text) == 2:
            return True
        testit = eval(answers.text)
        rightnow = datetime.utcnow()
        stripped = strptime(testit[0]['lastChangeTime'], "%Y-%m-%dT%H:%M:%S")
        rightthen = datetime.fromtimestamp(mktime(stripped))
        diff = rightnow - rightthen
        hours = int((diff.days*86400 + diff.seconds) / 3600)
        if hours > 2:
            return False
        return True


if __name__ == '__main__':
    allc = MoniLTA()
    allc.WriteStatusFile()
