''' mounter.py  mount the disks in the slots (5 is bad) and check the
    UUIDs to see if any of them are already dumped '''
import os
import re
import sys
import requests
import Utils as U

DEBUG_JNB = False

class Mounter():
    ''' Mount the disks in the slots and see if any are already
        accounted for '''
    def __init__(self):
        self.broken = [5]
        self.numberslots = 27
        self.range = []
        for i in range(1, self.numberslots + 1):
            if i not in self.broken:
                self.range.append(i)
        if os.path.isfile('/usr/bin/sudo'):
            self.execsudo = '/usr/bin/sudo'
        else:
            self.execsudo = '/bin/sudo'
        if os.path.isfile('/usr/bin/ls'):
            self.execls = '/usr/bin/ls'
        else:
            self.execls = '/bin/ls'
        if os.path.isfile('/sbin/mount'):
            self.execmount = '/sbin/mount'
        else:
            self.execmount = '/bin/mount'
        if os.path.isfile('/bin/df'):
            self.execdf = '/bin/df'
        else:
            self.execdf = '/usr/bin/df'
        self.pattern = re.compile('.*-.*-.*-.*')
    #
    def GetDumpedUUIDs(self):
        ''' Fetch the old PoleDisk info, and return the UUIDs '''
        #+
        # Arguments:	None
        # Returns:	array of UUID's of disks that have been dumped
        # Side Effects:	None
        # Relies on:	U.UnpackDBReturnJson
        #-
        uuidlist = []
        answer_get = requests.get(U.curltargethost + '/dumping/poledisk')
        answer = U.UnpackDBReturnJson(answer_get.text)
        for disk in answer:
            if disk['status'] == 'Done':
                uuidlist.append(disk['diskuuid'])
        return uuidlist
    #
    def MountIfNeeded(self):
        ''' Mount the disks if we need to.  Skip the broken slots '''
        #+
        # Arguments:	None
	# Returns:	boolean, False if there was a problem
        # Side Effects:	mounts disks if possible, does a df as well
        # Relies on:	U.getoutputerrorsimplecommand
        #-
        command = [self.execdf]
        # Use a 2-second wait
        answer, error, code = U.getoutputerrorsimplecommand(command, 2)
        if code != 0 or len(error) > 2:
            print('mounter::MountIfNeeded  failed the initial df', answer, error, code)
            return False
        lines = answer.split('\n')
        already = []
        for line in lines:
            if 'slot' in line:
                already.append(line.split()[-1])
        # These are already sorted by df, so don't fight with that
        todo = []
        if len(already) != len(self.range):
            for idx in self.range:
                exp = '/mnt/slot' + str(idx)
                if exp in already:
                    continue
                todo.append(exp)
        #
        if len(todo) <= 0:
            return True  # nothing to do, all is well
        # Do the mounting
        for slot in todo:
            command = [self.execsudo, self.execmount, slot]
            answer, error, code = U.getoutputerrorsimplecommand(command, 2)
            if code != 0 or len(error) > 2:
                print('mounter::MountIfNeeded  failed the initial df', answer, error, code)
                return False
        #
        return True
    #
    def SearchForUUIDs(self, slotnames):
        ''' Look for UUIDs in the slots '''
        #+
        # Arguments:	array of /mnt/slot1 etc
        # Returns:	array of [slot names,UUIDs on those slots] pairs
        # Side Effects:	ls of the slots, if possible
        # Relies on:	U.getoutputerrorsimplecommand
        #-
        uuids = []
        for slot in slotnames:
            # a priori knowledge:  the UUID has four dashes in it
            command = [self.execls, slot]
            answer, error, code = U.getoutputerrorsimplecommand(command, 2)
            if code != 0 or len(error) > 2:
                print('mounter::SearchForUUIDs failed to read the disk', answer, error, code)
                return uuids
            lines = answer.split('\n')
            for line in lines:
                if bool(self.pattern.search(line.split()[-1])):
                    uuids.append([slot, line.split()[-1]])
                    break
        return uuids
    #
    def MountAndCheck(self):
        ''' Driver routine for the mount of slots and verification that none are duplicates '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	mounts disks, reads disks, reads REST server, prints if problems
        # Relies on:	SearchForUUIDs
        #		MountIfNeeded
        #		GetDumpedUUIDs
        #		U.getoutputerrorsimplecommand
        #-
        if not self.MountIfNeeded():
            sys.exit(1)
        # Do another df to see what we have
        command = [self.execdf]
        # Use a 2-second wait
        answer, error, code = U.getoutputerrorsimplecommand(command, 2)
        if code != 0 or len(error) > 2:
            print('mounter::MountAndCheck  failed the second df', answer, error, code)
            sys.exit(2)
        lines = answer.split('\n')
        todo = []
        for line in lines:
            if 'slot' in line:
                todo.append(line.split()[-1])
        slot_uuid_seen = self.SearchForUUIDs(todo)
        uuid_done = self.GetDumpedUUIDs()
        for pair in slot_uuid_seen:
            if pair[1] in uuid_done:
                print('MountAndCheck: Alert', pair[0], 'has already been read, uuid', pair[1])

if __name__ == '__main__':
    mac = Mounter()
    mac.MountAndCheck()
