''' Use post-processing as a separate file Poster.py 
   Intended to be called at the end of the bash dump script,
   using an argument provided to the dump script '''
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
        # Relies on:	DumpControl.GiveTarget
        #		DumpControl.RetrieveDesiredTrees
        #		program called with first argument matching
        #		 the name of a source directory (begine with the disk name)
        #-
        if len(sys.argv) > 1:
            chunks = str(sys.argv[1]).split('/')
            if len(chunks) < 3:
                print('Cannot parse the slot name from ', sys.argv[1])
                sys.exit(1)
            self.sourcedir = '/' + chunks[1] + '/' + chunks[2] + '/'
        else:
            print('Need a directory based on the slot name')
            sys.exit(1)
        self.target = U.GiveTarget()
        self.listOfTops = U.RetrieveDesiredTrees()

    
    
    def FindOriginal(self):
        ''' Run a find on the files in the disk '''
        #+
        # Arguments:	None
        # Returns:	list of file names found.  Can be quite large
        # Side Effects:	executes a find in the specified directory
        #		It only takes 2 minutes to search the tree!
        #		print and die if error, exit 2
        # Relies on:	Utils.getoutputsimplecommand
        #-
        # Sanity
        if len(self.listOfTops) == 0:
            return []
        command = ['/usr/bin/find', '-f', self.sourcedir]
        hugelist, foerror, focode = U.getoutputerrorsimplecommand(command, 120)
        if focode != 0:
            print('FindOriginal failed to find the files in ', self.sourcedir, foerror, focode)
            sys.exit(2)
        #
        interesting = []
        if len(hugelist) <= 0:
            return interesting
        for entry in hugelist:
            for tree in self.listOfTops:
                if tree in entry:
                    interesting.append(entry)
                    break
        return interesting
    
    def RenameOne(self, foundDiskFile):
        ''' Given a file name of file found on the pole disk, strip off the
            disk name and replace it with the target directory name.
            Decide on  new name--if it is the same there's nothing to do.
            Otherwise attempt a mv of the old name to the new.  Die on failure '''
        #+
        # Arguments:	name of the file as found on the pole disk
        # Returns:		Nothing
        # Side Effects:	change the name of a file in the warehouse
        # Relies on:	NormalName
        #			Utils.getoutputsimplecommand
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
            command = ['/usr/bin/mv', tempName, newName]
            routp, rerr, rcode = U.getoutputerrorsimplecommand(command, 1)
            if rcode != 0:
                print('RenameOne failed during rename', tempName, newName, routp, rerr, rcode)
                sys.exit(3)
        except:
            print('RenameOne failed to rename', tempName, newName, routp, rerr, rcode)
            sys.exit(3)
        return
    #
    def ExecuteJob(self):
        ''' Execute the rename operations '''
        #+
        # Arguments:	None
        # Returns:		None
        # Side Effects:	Executes find and multiple mv's in filesystems
        #			Print and die on failure
        # Relies on:	RetrieveNames
        #			FindOriginal
        #			RenameOne
        #			sample directory on pole disk as argument
        #-
        listOfFiles = self.FindOriginal()
        for fname in listOfFiles:
            self.RenameOne(fname)
    #
    #####
    # main
    #ExecuteJob()
