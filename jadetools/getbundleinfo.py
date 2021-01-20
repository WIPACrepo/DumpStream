import os
import sys
#import glob
import datetime
import json
import sqlite3
import requests

# Utility for massaging database return info
def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))

#
def deltaT(oldtimestring):
    ''' Return the difference in time between the given time and now '''
    #+
    # Arguments:	old time in '%Y-%m-%d %H:%M:%S' format
    # Returns:		integer time difference in minutes
    # Side Effects:	None
    # Relies on:	Nothing
    #-
    current = datetime.datetime.now()
    try:
        oldt = datetime.datetime.strptime(oldtimestring, '%Y-%m-%d %H:%M:%S')
        difference = current - oldt
        delta = int(difference.seconds/60 + difference.days*60*24)
    except:
        delta = -1
    return delta


class BearerAuth(requests.auth.AuthBase):
    ''' Utility class for using the token with requests package '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

class BunCheck():
    ''' Class to handle dumped file deletion '''
    def __init__(self, name='service-token', donename='DONELIST', quarfile='QUARANTINE.INFO', histfile='statuses.json', reqfile='DONEREQUEST.LIST'):
        ''' __init__ for BunCheck; load LTA token '''
        #+
        # Arguments:	optional name of service token file; default service-token
        # Returns:	Nothing
        # Side Effects: Subroutine reads a file
        # Relies on:	getLTAToken
        #		ReadConfig
        #-
        token = self.getLTAToken(name)
        self.bearer = BearerAuth(token)
        self.apriori = ['PFRaw', 'PFDST', 'pDAQ-2ndBld']
        self.bstati = {}
        # Note that "quarantine" is not "quarantined".  That's deliberate, so I don't match the keyword.
        for status in ['specified', 'created', 'staged', 'transferring', 'taping', 'verifying', 'completed', 'detached', 'source-deleted', 'deleted', 'finished', 'quarantine']:
            self.bstati[status] = 0
        # This doesn't look promising by itself, with only the LTA server's info
        self.rstati = {}
        for status in ['completed', 'deprecated', 'processing', 'quarantined']:
            self.rstati[status] = 0
        self.histfile = histfile
        self.quarfile = quarfile
        self.quarinfo = []
        # Read in done bundle info
        self.doneuuid = []
        self.donename = donename
        try:
            fhandle = open(donename, 'r')
        except:
            print('BunCheck:__init__ failed to open', donename)
            sys.exit(1)
        try:
            buncholines = fhandle.readlines()
            fhandle.close()
            for line in buncholines:
                self.doneuuid.append(line.rstrip())
        except:
            print('BunCheck:__init__ failed to read', donename)
            sys.exit(1)
        # Read in finished requests list
        self.donerequuid = []
        self.reqfile = reqfile
        try:
            fhandle = open(reqfile, 'r')
        except:
            print('BunCheck:__init__ failed to open request file', reqfile)
            sys.exit(1)
        try:
            buncholines = fhandle.readlines()
            fhandle.close()
            for line in buncholines:
                self.donerequuid.append(line.rstrip())
        except:
            print('BunCheck:__init__ failed to read request file', reqfile)
            sys.exit(1)
        try:
            self.sqlitedb = sqlite3.connect('lastuse.db')
            self.sqlitedb.row_factory = make_dicts
        except:
            print('BunCheck:__init__ failed to read lastuse.db')
            sys.exit(1)
        # Dumper info:  abandoned filesdeleted finished LTArequest neverdelete processing unclaimed
        # From FullDirectory
        # Only unclaimed, processing, and LTArequest are significant in looking for bottlenecks.
        # This DB is keyed by idealName (of the directory), so I need to preserve the TransferRequest uuid and path too
        # The bundles should preserve the uuid and transfer request--I can key back to the transfer request for the path
        # LTArequest doesn't seem to be kept as up-to-date as it ought.  However, modern versions include the 
        # transfer request, and I can cross-check that to see what state that is in the LTA.  Or I can try to bolt on
        # some updating code somewhere, which might be simpler to produce and maintain.
        #

    #
    def query_db(self, query, args=(), one=False):
        ''' Query the sqlite3 db.  Do not close the connection '''
        # If the DB wasn't opened, the program should have exited already
        try:
            cur = self.sqlitedb.execute(query, args)
        except sqlite3.Error as e:
            self.sqlitedb.close()
            print('BunCheck:query_db failure', query, args, e)
            sys.exit(2)
        rv = cur.fetchall()
        cur.close()
        return (rv[0] if rv else None) if one else rv
    #
    def insert_db(self, query, args=()):
        ''' Insert into or update the sqlite3 db.  Do not close the connection '''
        # If the DB wasn't opened, the program should have exited already
        try:
            cur = self.sqlitedb.execute(query, args)
        except sqlite3.Error as e:
            self.sqlitedb.close()
            print('BunCheck:insert_db failure', query, args, e)
            sys.exit(2)
        #
        _ = cur.fetchall()
        self.sqlitedb.commit()
        cur.close()
    #
    def getLTAToken(self, tokenfilename):
        ''' Read the LTA REST server token from file "tokenfilename"; set it for the class '''
        #+
        # Arguments:	token file name string
        # Returns:	token
        # Side Effects:	reads a file, if possible
        # Relies on:	file with token
        #-
        try:
            tf = open(tokenfilename, 'r')
            token = tf.readline().strip()
            tf.close()
            return token
        except Exception as e:
            print('getLTAToken failed to read from', tokenfilename, e)
            sys.exit(1)
    #
    def WriteNewDone(self):
        ''' Write out a new done list, overwriting the old '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	writes a new version of the donefile
        # Relies on:	Nothing
        #-
        self.doneuuid.sort()
        try:
            fhandle = open(self.donename, 'w')
        except:
            print('BunCheck:WriteNewDone failed to open for write', self.donename)
            sys.exit(2)
        try:
            for uuid in self.doneuuid:
                fhandle.write(uuid + '\n')
            fhandle.close()
        except Exception as e:
            print('BunCheck:WriteNewDone failed in writing', self.donename, e)
            sys.exit(2)
    #
    def GetBundleLs(self):
        ''' Find the new bundles not known to be done yet '''
        #+
        # Arguments:	None
        # Returns:	array of uuid
        # Side Effects:	Call to REST server
        # Relies on:	REST server
        #-
        try:
            #bunlist = requests.get('https://lta.icecube.aq/Bundles?query', auth=self.bearer)
            bunlist = requests.get('https://lta.icecube.aq/Bundles?contents=0', auth=self.bearer)
        except Exception as e:
            print('getbundleinfo: GetBundleLs failed to get a list of bundles', e)
            sys.exit(4)
        try:
            thisb = bunlist.json()['results']
        except Exception as e:
            print('getbundleinfo: GetBundleLs failed to read the list of bundles', e, bunlist.text)
            sys.exit(4)
        newuuid = []
        for js in thisb:
            if js not in self.doneuuid:
                newuuid.append(js)
        return newuuid
    #
    def GetBundleInfo(self, uuid):
        ''' Load a dictionary with status information '''
        #+
        # Arguments:	uuid of bundle
        # Returns:	Nothing
        # Side Effects:	Call to REST server
        # Relies on:	REST server
        #-
        donetypes = ['finished', 'deprecated', 'abandoned', 'external']
        bunlist = requests.get('https://lta.icecube.aq/Bundles/' + str(uuid), auth=self.bearer)
        thisb = bunlist.json()
        status = thisb['status']
        # INSERT CHECK AGAINST OTHER DB
        query = 'SELECT * FROM bundle WHERE buuid=?'
        args = (uuid,)
        localanswer = self.query_db(query, args)
        # Different actions depending on whether it is present in the rapid db or not
        if len(localanswer) == 0:
            # load the current info into the database if it isn't there
            xclaim = ''
            if thisb['claimed'] == 'true':
                xclaim = thisb['claimant']
            erro = ''
            args = (uuid, thisb['path'], thisb['status'], thisb['request'], xclaim, erro)
            querya = 'INSERT INTO bundle (buuid,path,laststatus,lastchangetime,requestuuid,claimant,error)'
            queryb = ' VALUES (?,?,?,datetime(\'now\',\'localtime\'),?,?,?)'
            try:
                self.insert_db(querya + queryb, args)
            except Exception as e:
                print('GetBundleInfo: Failed to update_db with initial entry', e)
                sys.exit(4)
        else:
            # The current bundle is in the database, check its status
            localans = localanswer[0]
            if localans['laststatus'] == thisb['status']:
                # Nothing has changed
                if status in donetypes:
                    if uuid not in self.doneuuid:
                        self.doneuuid.append(uuid)
                    return
                # Check how long it has been.
                deltat = deltaT(localans['lastchangetime'])
                if deltat > 86400:
                    print('Bundle', uuid, 'has been in', status, 'for', deltat, 'seconds')
                    return
        # If this bundle is done, add it to the donelist that we'll write out later
        if status in donetypes:
            self.doneuuid.append(thisb['uuid'])
            return
        # Special case.  deleted may not transition to finished if the files are NOT TO BE DELETED
        if status == 'deleted':
            dones = True
            for statype in self.apriori:
                if statype in thisb['path']:
                    dones = False
                    break
            if dones:
                self.doneuuid.append(thisb['uuid'])
                return
        # Load info for quarantined files.  Write these out at the end
        if status == 'quarantined':
            if thisb['claimed'] == 'false':
                claimant = 'unclaimed'
            else:
                claimant = thisb['claimant']
            self.quarinfo.append([uuid, claimant])
            return
        # Histogram bait.
        self.bstati[status] = self.bstati[status] + 1
    #
    def WriteEvent(self):
        ''' Write out the histogram bait, the quarantine info, and the new donelist '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Writes out 3 files (append, append, re-write)
        # Relies on:	WriteNewDone (re-writes the donefile)
        #-
        self.WriteNewDone()
        #
        # append the quarantine info, if any
        if len(self.quarinfo) > 0:
            try:
                fhandle = open(self.quarfile, 'a')
            except:
                print('BunCheck:WriteEvent failed to open quarinfo file', self.quarfile)
                sys.exit(3)
            try:
                for pair in self.quarinfo:
                    fhandle.write(pair[0] + ' ' + pair[1] + '\n')
                fhandle.close()
            except Exception as e:
                print('BunCheck:WriteEvent failed to write to quarinfo file', self.quarfile, e)
                sys.exit(3)
        #
        # append the json histogram bait
        try:
            # Count the number of quarantined bundles
            self.bstati['quarantine'] = len(self.quarinfo)
            outfile = open(self.histfile, 'a')
            outfile.write(',')
            json.dump(self.bstati, outfile)
            outfile.write('\n')
            outfile.close()
            #with open(self.histfile, 'a') as outfile:
            #    json.dump(self.bstati, outfile)
        except Exception as e:
            print('BunCheck:WriteEvent failed to write to the histo file', self.histfile, e)
            sys.exit(3)
        #
    #
    def DriveReadIn(self):
        ''' Run the input from the REST server '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Reads from REST server
        # Relies on:	GetBundleLs
        #		GetBundleInfo
        #-
        array = self.GetBundleLs()
        if len(array) <= 0:
            return
        for uuid in array:
            self.GetBundleInfo(uuid)
    #
    ####
    #
    #
    #
    #
    #

if __name__ == '__main__':
    app = BunCheck()
    app.DriveReadIn()
    app.WriteEvent()
