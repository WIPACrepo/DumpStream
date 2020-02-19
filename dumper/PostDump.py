''' Use post-processing as a separate file PostDump.py 
   Intended to be called at the end of the bash dump script,
   using an argument provided to the dump script '''
import sys
import os
import Utils as U
import DumpControl as W

def RetrieveNames():
    ''' Get and return the diskname, the target directory top, and
        the list of tree-top fragments representing the desired data
        trees '''
    #+
    # Arguments:	slot number
    # Returns:		target, list of tops, name of slot
    # Side Effects:	None
    # Relies on:	REST server working
    #			GiveTarget
    #			RetrieveDesiredTrees
    #			program called with the name of a source directory
    #-
    #
    target = W.GiveTarget()
    listOfTops = W.RetrieveDesiredTrees()
    #
    if len(sys.argv) <= 1:
        # parse loose the upper portion of the source directory
        chunks = str(sys.argv[1]).split('/')
        if len(chunks) < 3:
            print('Cannot parse the slot name from ', sys.argv[1])
            sys.exit(1)
        sourcedir = '/' + chunks[1] + '/' + chunks[2] + '/'
    else:
        print('Need a directory based on the slot name')
        sys.exit(1)
    return target, listOfTops, sourcedir

def NormalName(filename):
    ''' Return the base file name without any ukey prefix.  May be expanded
        with new prefixes to remove as needed '''
    #+
    # Arguments:	file name
    # Returns:		base file name without cruft
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    # Assumption:  some files have ukey_${UUID}_rest-of-the-file-name
    #   we want 'rest-of-the-file-name'
    localstr = str(filename)
    directorypart = os.path.dirname(localstr)
    basepart = os.path.basename(localstr)
    chunks = basepart.split('_')
    nch = len(chunks)
    if chunks[0] != 'ukey':
        return localstr
    nna = ''
    for i in range(2, nch-1):
        nna += chunks[i] + '_'
    nna += chunks[nch-1]
    return directorypart + '/' + nna

def FindOriginal(diskname, desiredtrees):
    ''' Run a find on the files in the disk '''
    #+
    # Arguments:	directory name (e.g. /mnt/slot6)
    #			list of trees we care about (e.g. IceCube/YEAR/unbiased/PFRaw)
    # Returns:		list of file name found.  Can be quite large
    # Side Effects:	executes a find in the specified directory
    #			It only takes 2 minutes to search the tree!
    #			print and die if error, exit 2
    # Relies on:	Nothing
    #-
    # Sanity
    if len(desiredtrees) == 0:
        return []
    command = ['/usr/bin/find', '-f', diskname]
    hugelist, foerror, focode = U.getoutputerrorsimplecommand(command, 120)
    if focode != 0:
        print('FindOriginal failed to find the files in ', diskname, foerror, focode)
        sys.exit(2)
    #
    interesting = []
    if len(hugelist) <= 0:
        return interesting
    for entry in hugelist:
        for tree in desiredtrees:
            if tree in entry:
                interesting.append(entry)
                break
    return interesting

def RenameOne(foundDiskFile, diskName, targetName):
    ''' Given a file name of file found on the pole disk, strip off the
        disk name and replace it with the target directory name.
        Decide on  new name--if it is the same there's nothing to do.
        Otherwise attempt a mv of the old name to the new.  Die on failure '''
    #+
    # Arguments:	name of the file as found on the pole disk
    #			the pole disk name (e.g. /mnt/slot7)
    #			target name (e.g. /mnt/lfss/exp )
    # Returns:		Nothing
    # Side Effects:	change the name of a file in the warehouse
    # Relies on:	NormalName
    #-
    try:
        tempName = foundDiskFile.replace(diskName, targetName)
        newName = NormalName(tempName)
    except:
        print('RenameOne failed to generate new name', foundDiskFile, diskName, targetName)
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
#RetrieveNames()
#NormalName(filename)
#FindOriginal(diskname, desiredtrees)
#RenameOne(foundDiskFile, diskName, targetName)
def ExecuteJob():
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
    target, listOfTops, sourcedir = RetrieveNames()
    listOfFiles = FindOriginal(sourcedir, listOfTops)
    for fname in listOfFiles:
        RenameOne(fname, sourcedir, target)
#
#####
# main
ExecuteJob()
