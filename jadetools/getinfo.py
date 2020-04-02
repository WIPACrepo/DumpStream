''' Get info about contents and side effects in more or less simple form '''
import sys

def checkline(myline, commentflag, infoflag):
    mline = myline.strip()
    if mline[0:4] == 'def ':
        print('')
        return True, commentflag, infoflag
    if mline[0:2] == '#+':
        return False, commentflag, True
    if mline[0:2] == '#-':
        return False, commentflag, False
    nchunks = myline.split('\'\'\'')
    if len(nchunks) == 3:
        return True, False, infoflag
    if len(nchunks) == 2:
        if commentflag:
            return True, False, infoflag
        return True, True, infoflag
    if len(nchunks) == 1:
        myans = infoflag or commentflag
        return myans, commentflag, infoflag
    return False, commentflag, infoflag

if len(sys.argv) <= 1:
    print(len(sys.argv), ' nothing to do')
    sys.exit(0)

filename = str(sys.argv[1])
try:
    fn = open(filename, 'r')
    lines = fn.readlines()
    commentstat = False
    infostat = False
    for line in lines:
        answer, c1, i1 = checkline(line, commentstat, infostat)
        if answer:
            print(line.strip())
        commentstat = c1
        infostat = i1
except:
    print('Failure opening/reading', filename)
    sys.exit(1)
fn.close()
