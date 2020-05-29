''' Called from dumpscript (bash script) with pairs of source/target
    directories as arguments.
    For each pair:
     Rework the target to be a canonical /data/exp form.
     Look in the source for subdirectories.
     In turn, append these subdirectories to the target and
      check if the resulting directory is full.
      IF SO, add this to FullDirectories.
'''

import sys
import copy
import json
import requests
import Utils as U

class findfull:
    ''' class findfull  Look up the newly touched directories; if full
        let FullDirectories know '''
    def __init__(self):
        ''' __init__ input the arguments '''
        #+
        # Arguments:    "self"
        # Returns:      None
        # Side Effects: None
        # Relies on:    Utils.IsDirectoryFull
        #               Utils.getoutputerrorsimplecommand
        #               REST server working
        #-
        self.pairs = []
        kpairs = len(sys.argv) - 1
        if kpairs <= 1:
            sys.exit(0)   # nothing to do
        #
        numpairs = int(kpairs/2)
        if 2*numpairs != kpairs:
            print("findfull.__init__ Arguments are not in source/target pairs")
            sys.exit(1)
        for count in range(numpairs):
            source = sys.argv[count*2+1]
            target = sys.argv[count*2+2]
            twords = target.split('/exp/')
            if len(twords) != 2:
                print('findfull.__init__  target should have /exp/ as part of the path', target)
                sys.exit(4)
            canon = '/data/exp/' + twords[1]
            self.pairs.append([source, canon])

    def FindDirs(self, sourcedir):
        ''' Do an ls to find the subdirectories touched '''
        #+
        # Arguments:    "self"
        #               directory we copied from.  Usually /mnt/slotx/etc
        # Returns:      list of subdirectory names
        # Side Effects: Does an ls on the source
        # Relies on:    nothing
        #-
        command = ['/bin/ls', sourcedir]
        answer, erro, code = U.getoutputerrorsimplecommand(command, 5)
        if code != 0:
            print('findfull.FindDirs Failed to read', sourcedir, answer, erro)
            sys.exit(2)
        return str(answer).split()

    def SetDir(self, newdir):
        ''' Tell REST server that this directory is ready for processing '''
        #+
        # Arguments:    "self"
        #               directory that is verified to be full
        # Returns:      nothing
        # Side Effects: change to REST server DB
        # Relies on:    REST server working
        #-
        # Check if this exists already
        md = {}
        md['likeIdeal'] = newdir
        mangled = U.mangle(json.dumps(md))
        answers = requests.get(U.curltargethost + '/directory/info/' + mangled)
        if 'FAILURE' in answers.text:
            print('findfull.SetDir could not read', newdir)
            sys.exit(3)
        results = U.UnpackDBReturnJson(answers.text)
        # Do we have any entries?
        if results is not None:
            if len(results) <= 0:
                return
        mangled = U.mangle(newdir)
        answers = requests.post(U.curltargethost + '/directory/' + mangled)
        if 'FAILURE' in answers.text:
            print('findfull.SetDir could not load', newdir)
            sys.exit(3)
        sdposturl = copy.deepcopy(U.basicposturl)
        sdposturl.append(U.targetdumpingenteredreadydir + U.mangle(newdir))
        sdoutp, sderro, sdcode = U.getoutputerrorsimplecommand(sdposturl, 1)
        if sdcode != 0 or 'FAILURE' in str(sdoutp):
            print('findfull.SetDir could not load', newdir, sderro, sdoutp)
            sys.exit(3)
    
    def CheckDirs(self):
        ''' Loop over the pairs to see which newly copied directories are full '''
        #+
        # Arguments:    "self"
        # Returns:      nothing
        # Side Effects: file system access and DB access under the covers
        # Relies on:    FindDirs
        #               SetDir
        #-
        if len(self.pairs) == 0:
            return
        for pair in self.pairs:
            subdirlist = self.FindDirs(pair[0])
            tag = pair[0].split('/')[-1]
            if len(subdirlist) > 0:
                for subdir in subdirlist:
                    newdir = pair[1] + '/' + tag + '/' + subdir
                    if U.IsDirectoryFull(newdir):
                        self.SetDir(newdir)

####
# main
if __name__ == '__main__':
    app = findfull()
    app.CheckDirs()
