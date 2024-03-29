# Command.py (.base)
import sys
import copy
import Utils as U

DEBUGIT = False


########################################################
# Define Phases for Main.  In this case, all I care about
# is control of the NERSC and Local control systems.  It
# turns out NERSC has some issues with keeping the power
# on (thank you, PG&E), and it is handy to be able to
# turn everything off or on again.

def Phase1(pcarg):
    #
    lcarg = pcarg
    geturl = copy.deepcopy(U.basicposturl)
    geturl.append(U.targetsetdumpstatus + U.mangle(lcarg))
    answer1, erro1, code1 = U.getoutputerrorsimplecommand(geturl, 1)
    answer = U.massage(answer1)
    if 'OK' not in answer:
        print('Phase 1 failure with', geturl, answer, erro1, code1)
        return
    #
    if lcarg == 'Drain':
        lcarg = 'DrainNERSC'
    geturl = copy.deepcopy(U.basicposturl)
    geturl.append(U.targetsetnerscstatus + U.mangle(lcarg))
    answer1, erro1, code1 = U.getoutputerrorsimplecommand(geturl, 1)
    answer = U.massage(answer1)
    if 'OK' not in answer:
        print('Phase 1 failure with', geturl, answer, erro1)
    return

###############
# Main

if len(sys.argv) == 1:
    print('No command given.  Run, Halt, Drain are possible')
    sys.exit(0)

carg = str(sys.argv[1])

if carg not in ['Run', 'Halt', 'Drain']:
    print('Command ' + carg + ' is not in Run, Halt, Drain; bailing')
    sys.exit(1)

Phase1(carg)
sys.exit(0)
