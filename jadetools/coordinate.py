''' To run in a cron on lta-vm-2 as jadelta.
    Communicates w/ cluster nodes
    Keeps the appropriate number of bundler and checksum
     jobs running. '''
import Utils as U

class coordinate():
    ''' Ping cluster; query cluster; compare with the desired;
        execute on cluster (using sudo to jadelta) '''
    def __init__(self):
        self.workerpool = []
        self.workerpool.append('c9-1.icecube.wisc.edu')
        #hardware problem self.workerpool.append('c9-2.icecube.wisc.edu')
        self.workerpool.append('c9-3.icecube.wisc.edu')
        self.workerpool.append('c9-4.icecube.wisc.edu')
        self.workerpool.append('c9-5.icecube.wisc.edu')
        self.workerpool.append('c9-6.icecube.wisc.edu')
        #self.workerpool.append('c9-7.icecube.wisc.edu')  #lfs7 mount issue
        self.workerpool.append('c9-8.icecube.wisc.edu')
        self.workerpool.append('c9-9.icecube.wisc.edu')
        self.workerpool.append('c9-10.icecube.wisc.edu')
        self.workerpool.append('c9-11.icecube.wisc.edu')
        self.workerpool.append('c9-12.icecube.wisc.edu')
        self.workerpool.append('c8-5.icecube.wisc.edu')
        self.workerpool.append('c8-6.icecube.wisc.edu')
        self.workerpool.append('c8-7.icecube.wisc.edu')
        self.workerpool.append('c8-8.icecube.wisc.edu')
        self.workerpool.append('c8-9.icecube.wisc.edu')
        self.workerpool.append('c8-10.icecube.wisc.edu')
        self.workerpool.append('c8-11.icecube.wisc.edu')
        self.workerpool.append('c8-12.icecube.wisc.edu')
        self.moduleInfo = {}
        self.moduleInfo['bundler'] = [5, 'bundlerboost', 0]
        self.moduleInfo['check'] = [5, 'checkboost', 0]
        self.moduleInfo['delete'] = [1, 'delboost', 0]
        #self.moduleInfo['site-move-verifier-return'] = [1, 'smrboost', 0]
        #self.moduleInfo['delete-return'] = [1, 'delretboost', 0]
        self.moduleInfo['gridftp'] = [3, 'gridftpboost', 0]
        self.moduleInfo['ratelimit'] = [1, 'ratelimitboost', 0]
        self.moduleInfo['deletemadison'] = [1, 'delmadboost', 0]
        #self.moduleInfo['unpacker'] = [1, 'unpackboost', 0]
        self.candidatePool = []
        self.countActiveBundles = 0
        self.countActiveChecks = 0
        self.countActiveDeletes = 0
        self.countActiveUnpacker = 0
        self.countActiveSiteVerifier = 0
        self.countActiveDeletesReturn = 0
        self.countActiveGridftp = 0
        self.countActiveRatelimit = 0
        self.countLocalDelete = 0
        self.workerscripts = '/home/jadelta/dumpcontrol/DumpStream/'
        self.cmdping = '/bin/ping'
        self.cmdssh = '/usr/bin/ssh'
    #
    def GetCandidates(self):
        ''' Ping workerpool machines until we get a list of
             live ones long enough '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	multiple pings
        # Relies on:	Utils.getoutputerrorsimplecommand
        #-
        # How many do we need?
        targetCount = 0
        for p in self.moduleInfo:
            targetCount = targetCount + self.moduleInfo[p][0]
        targetCount = targetCount + 2
        # Find that many, if possible
        for host in self.workerpool:
            cmd = [self.cmdping, '-c1', '-w', '1', host]
            answer, _, code = U.getoutputerrorsimplecommand(cmd, 1)
            if code != 0:
                continue
            if 'Unreachable' in answer:
                continue
            self.candidatePool.append(host)
            if len(self.candidatePool) >= targetCount:
                break
    #
    def GetInUse(self):
        ''' Query each of the candidate hosts to see what jadelta
            jobs they are running.  Increment the counts of modules
            Note that this does not check modules running on other
            hosts.  I may change that in the future, since I don't
            want multiple instances of the deleter--though deleter
            is a fast module.
            Return a list of empty hosts '''
        #+
        # Arguments:	None
        # Returns:	list of empty hosts
        # Side Effects:	multiple calls of script in remote hosts
        # Relies on:	Utils.getoutputerrorsimplecommand
        #-
        emptyList = []
        for host in self.candidatePool:
            cmd = [self.cmdssh, 'jadelta@' + host, self.workerscripts + 'getmex']
            answer, _, code = U.getoutputerrorsimplecommand(cmd, 1)
            if code != 0:
                continue
            if 'InterfaceLTA' not in answer and 'bundler' not in answer and 'AutoFiles2' not in answer and 'delete-return' not in answer and 'unpacker' not in answer and 'gridftp' not in answer and 'site-move-verifier' not in answer and 'rate' not in answer and 'pipe0-deleter' not in answer:
                emptyList.append(host)
                continue
            if 'InterfaceLTA' in answer:
                self.countActiveChecks = self.countActiveChecks + 1
            if 'bundler' in answer:
                self.countActiveBundles = self.countActiveBundles + 1
            if 'AutoFiles2' in answer:
                self.countActiveDeletes = self.countActiveDeletes + 1
            if 'delete-return' in answer:
                self.countActiveDeletesReturn = self.countActiveDeletesReturn + 1
            if 'unpacker' in answer:
                self.countActiveUnpacker = self.countActiveUnpacker + 1
            if 'site-move-verifier' in answer:
                self.countActiveSiteVerifier = self.countActiveSiteVerifier + 1
            if 'gridftp' in answer:
                self.countActiveGridftp = self.countActiveGridftp + 1
            if 'ratelimit' in answer:
                self.countActiveRatelimit = self.countActiveRatelimit + 1
            if 'pipe0-deleter' in answer:
                self.countLocalDelete = self.countLocalDelete + 1
        self.moduleInfo['bundler'][2] = self.countActiveBundles
        self.moduleInfo['check'][2] = self.countActiveChecks
        self.moduleInfo['delete'][2] = self.countActiveDeletes
        #self.moduleInfo['unpacker'][2] = self.countActiveUnpacker
        #self.moduleInfo['site-move-verifier-return'][2] = self.countActiveSiteVerifier
        #self.moduleInfo['delete-return'][2] = self.countActiveDeletesReturn
        self.moduleInfo['gridftp'][2] = self.countActiveGridftp
        self.moduleInfo['ratelimit'][2] = self.countActiveRatelimit
        self.moduleInfo['deletemadison'][2] = self.countLocalDelete
        return emptyList
    #
    def Launch(self):
        ''' Find out which modules need to be launched and which
            hosts are free, and launch the modules 
            Driver program '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	multiple calls of script in remote hosts
        # Relies on:	Utils.getoutputerrorsimplecommand
        #		GetCandidates
        #		GetInUse
        #-
        self.GetCandidates()
        emptyList = self.GetInUse()
        # If there are no free hosts, we can't do anything anyway
        numberFree = len(emptyList)
        if numberFree <= 0:
            return
        whichHost = 0
        for module in self.moduleInfo:
            infom = self.moduleInfo[module]
            if infom[0] > infom[2]:
                # Fire off no more than 1 of each type at a time
                cmd = [self.cmdssh, 'jadelta@' + emptyList[whichHost], self.workerscripts + infom[1]]
                answer, error, code = U.getoutputerrorsimplecommand(cmd, 1)
                if code != 0:
                    print('coordinate::Launch', cmd, answer, error, code)
                    return
                whichHost = whichHost + 1
                # Are we out of free hosts?
                if whichHost > numberFree - 1:
                    return
        return

if __name__ == '__main__':
    launch = coordinate()
    launch.Launch()
