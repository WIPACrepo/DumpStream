import sys
import json
import copy

import Utils as U

USEHEARTBEATS = False

########################################################
# Main

# optional argument : dups

NagiosError = ''
NagiosWarning = ''
THRESHOLDS = {'Untouched':10, 'NERSCProblem':0, 'NERSCClean':20, 'LocalDeleted':20, 'unstaged':20}
nstats = 'DB'

####
# NERSC status   
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetnerscinfo)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' NERSC'
else:
    try:
        my_json = json.loads(U.singletodouble(outp))
        if str(my_json['status']) == 'Error':
            NagiosError = NagiosError + 'NERSC+'
        if int(deltaT(str(my_json['lastChangeTime']))) > 18000:
            NagiosWarning = NagiosWarning + 'NERSCLate+'
    except:
        nstats = nstats + ' NERSC'


####
# local status   
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpinfo)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' Bundle'
else:
    try:
        my_json = json.loads(U.singletodouble(outp))
        if str(my_json['status']) == 'Error':
            NagiosError = NagiosError + 'BundleScanner+'
        if int(deltaT(str(my_json['lastChangeTime']))) > 18000:
            NagiosWarning = NagiosWarning + 'BundleScannerLate+'
    except:
        nstats = nstats + ' Bundle'

####
# Disk dumping status
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpingstate)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' Dumping'
else:
    try:
        my_json = json.loads(U.singletodouble(outp))
        myj = my_json[0]
        if str(myj['status']) == 'Error':
            NagiosError = NagiosError + 'Dumper+'
    except:
        nstats = nstats + ' Dumping'

####
# FullDirectories available

geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpingcountready)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + ' FullDir'
else:
    try:
        my_json = json.loads(U.singletodouble(outp))[0]
        if int(my_json['unstaged']) > THRESHOLDS['unstaged']:
            NagiosWarning = NagiosWarning + 'FDunstaged+'
    except:
        nstats = nstats + ' FullDir'


####
# How many bundles have each status?
# I will probably get fancier later.  For now, just this.
for opt in THRESHOLDS:
    if opt == 'unstaged':
        continue
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetbundlestatuscount + U.mangle(opt))
    outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    #print(outp)
    if int(code) != 0:
        nstats = nstats + ' ' + opt
    else:
        try:
            my_json = json.loads(U.singletodouble(outp))
            js = my_json[0]
            if int(js['COUNT(*)']) > THRESHOLDS[opt]:
                NagiosWarning = NagiosWarning + ' ' + opt
        except:
            nstats = nstats + ' ' + opt

if NagiosError != '':
    NagiosError = NagiosError[0:-1]
if NagiosWarning != '':
    NagiosWarning = NagiosWarning[0:-1]

if NagiosError != '':
    print('toNERSC CRIT |', NagiosError, '|')
    sys.exit(2)

if nstats != 'DB':
    print('toNERSC UNKNOWN |', nstats, '|')
    sys.exit(3)

if NagiosWarning != '':
    print('toNERSC WARN |', NagiosWarning, '|')
    sys.exit(1)

print('toNERSC OK | |')
sys.exit(0)
