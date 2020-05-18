''' Use post-processing as a separate file renamer.py 
   Intended to be called at the end of the bash dump script,
   using an argument provided to the dump script.
   This is called from the dumpscript bash script, after
   the rsync's complete.  '''
import os
import sys
import Utils as U

class renamer:
    ''' class renamer.  Look up the files, figure the right names, rename them '''
    def __init__(self):
        ''' __init__ : Get the disk name, list of desired top directories, and
            the target directory to which things were dumped '''
        #+
        # Arguments:	"self"
        # Returns:	None
        # Side Effects:	None
        # Relies on:	Utils.GiveTarget  (REST request)
        #		Utils.RetrieveDesiredTrees (REST request)
        #		program called with first argument matching
        #		 the name of a source directory (begin with the disk name)
        #-
        if len(sys.argv) > 1:
            chunks = str(sys.argv[1]).split('/')
            if len(chunks) < 3:
                print('Cannot parse the slot name from ', sys.argv[1])
                sys.exit(1)
            self.sourcedir = '/' + chunks[1] + '/' + chunks[2] + '/'
            self.sourcedir.replace('//', '/')
        else:
            print('Need a directory based on the slot name')
            sys.exit(1)
        self.target = U.GiveTarget() + '/'
        self.target.replace('//', '/')
        self.listOfTops = U.RetrieveDesiredTrees()
        if os.path.isfile('/bin/mv'):
            self.execmv = '/bin/mv'
        else:
            self.execmv = '/usr/bin/mv'
        if os.path.isfile('/bin/chown'):
            self.execchown = '/bin/chown'
        else:
            self.execchown = '/usr/bin/chown'

    
    def FindOriginal(self):
        ''' Run a find on the files in the disk '''
        #+
        # Arguments:	None
        # Returns:	list of file names found.  Can be quite large
        # Side Effects:	executes a find in the specified directory
        #		It only takes 2 minutes to search the tree!
        #		print and die if error, exit 2
        # Relies on:	Utils.getoutputsimplecommand (find)
        #-
        # Sanity
        if len(self.listOfTops) == 0:
            return []
        # Note that jade03 has /bin/find instead of /usr/bin/find
        # Note that I need to exclude lost+found!
        command = ['/bin/find', self.sourcedir, '-maxdepth', '1', '-mindepth', '1', '-type', 'd']
        dirlist, foerror, focode = U.getoutputerrorsimplecommand(command, 1)
        if focode != 0:
            print('FindOriginal failed to find the top directories in', self.sourcedir, foerror, focode)
            sys.exit(2)
        dirlists = dirlist.split()
        hugelist = []
        for directory in dirlists:
            if directory == self.sourcedir + 'lost+found':
                continue
            command = ['/bin/find', directory, '-type', 'f']
            biglist, foerror, focode = U.getoutputerrorsimplecommand(command, 120)
            if focode != 0:
                print('FindOriginal failed to find the files in ', directory, foerror, focode)
                sys.exit(2)
            hugelist = hugelist + biglist.split()
        #
        interesting = []
        if len(hugelist) <= 0:
            return interesting
        for entry in hugelist:
            for tree in self.listOfTops:
                if U.TreeComp(tree, entry):
                    interesting.append(entry)
                    break
        return interesting
    
    def RenameOne(self, foundDiskFile):
        ''' Given a file name of file found on the pole disk, strip off the
            disk name and replace it with the target directory name.
            Decide on  new name--if it is the same there's nothing to do.
            Otherwise attempt a mv of the old name to the new.  Die on failure
            EXPANDED SPECIFICATIONS
            Change the ownership to jadelta:jadelta '''
        #+
        # Arguments:	name of the file as found on the pole disk
        # Returns:	Nothing
        # Side Effects:	change the name of a file in the warehouse
        #		print error on failure--don't die; might be intermittent
        # Relies on:	Utils.NormalName
        #		Utils.getoutputsimplecommand (mv)
        #-
        try:
            tempName = foundDiskFile.replace(self.sourcedir, self.target)
            newName = U.NormalName(tempName)
        except:
            print('RenameOne failed to generate new name', foundDiskFile, tempName, newName)
            sys.exit(3)
        #
        if tempName == newName:
            return
        try:
            command = ['/usr/bin/sudo', self.execmv, tempName, newName]
            routp, rerr, rcode = U.getoutputerrorsimplecommand(command, 1)
            if rcode != 0:
                print('RenameOne failed during rename', tempName, newName, routp, rerr, rcode)
                #sys.exit(3)
        except:
            print('RenameOne failed to rename', tempName, newName, routp, rerr, rcode)
            #sys.exit(3)
        try:
            command = ['/usr/bin/sudo', self.execchown, 'jadelta:jadelta', newName]
            routp, rerr, rcode = U.getoutputerrorsimplecommand(command, 1)
            if rcode != 0:
                print('RenameOne failed during chown', newName, routp, rerr, rcode)
                #sys.exit(3)
        except:
            print('RenameOne failed to chown', newName, routp, rerr, rcode)
        return
    #
    def ChownDir(self, foundDiskFile):
        ''' From the list of files to manage, get a (much shorter) list of
            directories that need to be chown'ed for safety.
            chown them. '''
        #+
        # Arguments:	list of file names as found on the pole disk
        # Returns:	Nothing
        # Side Effects:	chown the directory immediately above the file
        # Relies on:	Nothing
        #-
        direList = []
        for dfile in foundDiskFile:
            ddir = os.path.dirname(dfile)
            if ddir not in direList:
                direList.append(ddir)
        for ddir in direList:
            try:
                command = ['/usr/bin/sudo', self.execchown, 'jadelta:jadelta', ddir]
                routp, rerr, rcode = U.getoutputerrorsimplecommand(command, 1)
                if rcode != 0:
                    print('ChownDir failed during chown', ddir, routp, rerr, rcode)
            except:
                print('ChownDir failed to chown', ddir, routp, rerr, rcode)
    #
    def ExecuteJob(self):
        ''' Execute the rename operations '''
        #+
        # Arguments:	None
        # Returns:	None
        # Side Effects:	Executes find and multiple mv's in filesystems
        #		Print and die on failure
        # Relies on:	RetrieveNames
        #		FindOriginal
        #		RenameOne
        #		sample directory on pole disk as argument (e.g. /mnt/slot8/IceCube/2018/etc)
        #-
        listOfFiles = self.FindOriginal()
        for fname in listOfFiles:
            self.RenameOne(fname)
#
#####
# main
if __name__ == '__main__':
    app = renamer()
    app.ExecuteJob()
