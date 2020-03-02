# Dirinfo.py (.base)
import sys
import json
import copy

import Utils as U


DEBUGIT = False


########################################################
# Define Phases for Main.  In this case, all I care about
#  is getting information from the database.

def Phase1(pcarg):
    #
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetfindbundleslike + U.mangle(pcarg))
    answer1, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    answer = U.massage(answer1)
    if len(answer) == 0:
        print('No answer for', pcarg, answer, erro, code)
        return
    try:
        janswer = json.loads(singletodouble(answer))
    except:
        print('Failed to create json:', pcarg, answer)
        return
    for j in janswer:
        print(j['idealName'], j['bundleStatus_id'], j['status'])
    #
    return

###############
# Main

if len(sys.argv) == 1:
    print('No directory/file specified.  Use the \"ideal\" name or part of the name')
    sys.exit(0)

for i in range(1, len(sys.argv)):
    carg = str(sys.argv[i])
    Phase1(carg)
sys.exit(0)
