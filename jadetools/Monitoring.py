import json
import copy
import os
import sys
import Utils as U

USEHEARTBEATS = False

########################################################
# Main

# optional argument : dups

####
# NERSC status   
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetnerscinfo)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    #print(outp)
    my_json = json.loads(U.singletodouble(outp))
    nstats = (str(my_json['status']) + ' | ' + str(my_json['nerscError']) + ' | '
              + str(my_json['nerscSize']) + ' | ' + str(my_json['lastChangeTime'])
              + '  ' + str(U.deltaT(str(my_json['lastChangeTime']))))
U.logit('NERSCStatus= ', nstats)


####
# NERSC token status
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targettokeninfo)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    my_json = json.loads(U.singletodouble(outp))
    tname = 'NULL'
    if my_json['hostname'] != '':
        tname = my_json['hostname']
    nstats = tname + ' at ' + str(my_json['lastChangeTime'])
    nstats = nstats + '  ' + str(U.deltaT(str(my_json['lastChangeTime'])))
U.logit('NERSCToken= ', nstats)

####
# NERSC heartbeats
if USEHEARTBEATS:
    geturl = copy.deepcopy(U.basicgeturl)
    geturl.append(U.targetheartbeatinfo)
    outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
    nstats = ''
    if int(code) != 0:
        nstats = 'DB Failure'
    else:
        trialbunch = U.stringtodict(str(outp))
        #print(trialbunch)
        nstats = 'Beats: '
        for chunk in trialbunch:
            #print(chunk)
            my_json = json.loads(U.singletodouble(chunk))
            nstats = nstats + '| ' + my_json['hostname'] + '::' + str(my_json['lastChangeTime'])
            nstats = nstats + '  ' + str(U.deltaT(str(my_json['lastChangeTime'])))
    U.logit('NERSCHeartbeats= ', nstats)


####
# local status   
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpinfo)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure'
else:
    #print(outp)
    my_json = json.loads(U.singletodouble(outp))
    nstats = (my_json['status'] + ' | ' + my_json['bundleError'] + ' | '
              + str(my_json['bundlePoolSize']) + ' | ' + str(my_json['lastChangeTime']))
    nstats = nstats + '  ' + str(U.deltaT(str(my_json['lastChangeTime'])))
U.logit('LocalStatus= ', nstats)

####
# Disk dumping status
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpingstate)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure w/ DiskDumping status'
else:
    try:
        my_json = json.loads(U.singletodouble(outp))
        myj = my_json[0]
        nstats = (str(myj['status']) +  '=> ' + str(myj['nextAction'])
                  + ' | ' + str(myj['lastChangeTime']))
    except:
        print(outp)
        print(myj)
        nstats = 'FAILURE'
U.logit('DiskDumpStatus= ', nstats)

####
# FullDirectories available

geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpingcountready)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure w/ FullDirectory'
else:
    my_json = json.loads(U.singletodouble(outp))[0]
    nstats = 'total=' + str(my_json['total']) + ' | ' + 'unstaged=' + str(my_json['unstaged'])
    nstats = nstats + ' | ' + 'staged= ' + str(my_json['staged'])
    nstats = nstats + ' | ' + 'done= ' + str(my_json['done'])
    recount = int(my_json['unstaged']) + int(my_json['staged']) + int(my_json['done'])
    if recount != int(my_json['total']):
        nstats = nstats + '  and ' + str(recount - int((my_json['total']))) + ' not accounted for'
U.logit('FullDirectories= ', nstats)

geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetdumpinggetwhat)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
nstats = ''
if int(code) != 0:
    nstats = 'DB Failure w/ DiskDumping'
else:
    my_json = json.loads(U.singletodouble(outp))
    for stype in U.PoleDiskStatusOptions:
        ccount = 0
        for ji in my_json:
            if ji['status'] == stype:
                ccount = ccount + 1
        nstats = nstats + ' | ' + stype + ':' + str(ccount)
U.logit('DiskStatuses= ', nstats)


####
# How many bundles have each status?
# I will probably get fancier later.  For now, just this.
nstats = ''
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetallbundleinfo)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    nstats = nstats + 'DB Failure'
else:
    try:
        my_json = json.loads(U.singletodouble(outp))
        js = my_json[0]
        for w in js:
            nstats = nstats + ' | ' + w + ':' + str(js[w])
    except:
        nstats = nstats + 'DB DID NOT GIVE GOOD JSON'
U.logit('BundleStatusCounts= ', nstats)


# Are we done?  Not if we want to look for duplicate entries.
if len(sys.argv) <= 1:
    sys.exit(0)
if 'dups' not in sys.argv[1]:
    sys.exit(0)
####
# Do we have duplicate entries?

doubles = -1
ndoubles = ''
geturl = copy.deepcopy(U.basicgeturl)
geturl.append(U.targetbundlesworking)
outp, erro, code = U.getoutputerrorsimplecommand(geturl, 1)
if int(code) != 0:
    ndoubles = 'DB Failure'
else:
    doubles = 0
    bunch = []
    my_json = json.loads(U.singletodouble(outp))
    for js in my_json:
        bunch.append([os.path.basename(js['localName']), str(js['status']), str(js['bundleStatus_id'])])
    for b in bunch:
        ln = b[0]
        for c in bunch:
            if c != b:
                if ln == c[0]:
                    doubles = doubles + 1
    doubles = doubles / 2
    ndoubles = str(doubles)
# do I want to log this at all if there's no problem?
if doubles != 0:
    U.logit('Duplicate bundle transfers=', ndoubles)
