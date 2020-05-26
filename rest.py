# rest.py
import os
import sys
import site
python_home = '/opt/testing/rest/venv'
#Calc the path to site_packages directory
python_version = '.'.join(map(str, sys.version_info[:2]))
site_packages = python_home + '/lib/python%s/site-packages' % python_version

## Add the site-packages directory.
#site.addsitedir(site_packages)

# Remember original sys.path.
if not site_packages in list(sys.path):
    site.addsitedir(site_packages)

#############################3

from flask import Flask
from flask import request
from config import Config
import sqlite3
import json
import urllib.parse

DATABASE = '/opt/testing/rest/db.test'
REPLACESTRING = '+++'
REPLACENOT = '==='
REPLACECURLLEFT = '+=+=+'
REPLACECURLRIGHT = '=+=+='
DUMPSTATI = ['Run', 'Halt', 'Drain', 'Error']
NERSCSTATI = ['Run', 'Halt', 'DrainNERSC', 'Error']
BUNDLESTATI = ['Untouched', 'JsonMade', 'PushProblem', 'PushDone',
               'NERSCRunning', 'NERSCDone', 'NERSCProblem', 'NERSCClean',
               'LocalDeleted', 'LocalFilesDeleted', 'Abort', 'Retry',
               'RetrieveRequest', 'RetrievePending', 'ExportReady',
               'Downloading', 'DownloadDone', 'RemoteCleaned']
WORKINGTABLESTATI = ['Unpicked', 'Preparing', 'Picked']
BUNDLESTATUSCOLUMNS = ['bundleStatus_id', 'localName', 'idealName', 'UUIDJade', 'UUIDGlobus', 'size', 'status', 'useCount', 'checksum', 'LooseFileDir', 'DownloadDir', 'FileDate']
PoleDiskStatusOptions = ['New', 'Inventoried', 'Dumping', 'Done', 'Removed', 'Error']
DumperStatusOptions = ['Idle', 'Dumping', 'Inventorying', 'Error']
DumperNextOptions = ['Dump', 'Pause', 'DumpOne', 'Inventory']
BUNDLECOLS = ["bundleStatus_id", "localName", "idealName", "UUIDJade", "UUIDGlobus", "size", "status", "useCount", "checksum", "LooseFileDir"]
FULL_DIR_STATI = ['unclaimed', 'processing', 'LTArequest', 'filesdeleted', 'problem', 'deprecated']
FULL_DIR_SINGLE_STATI = ['unclaimed', 'filesdeleted', 'problem', 'deprecated']
SLOTBAD = -2
SLOTRESERVED = -1
SLOTUNK = 0
DEBUGDB = False


# String manipulation stuff
def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING).replace('!', REPLACENOT)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/').replace(REPLACENOT, '!')

def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace(r'\,', ',').replace('@', ' ').replace(REPLACENOT, '!').replace(REPLACECURLLEFT, '{').replace(REPLACECURLRIGHT, '}')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', r'\,').replace(' ', '@').replace('!', REPLACENOT).replace('{', REPLACECURLLEFT).replace('}', REPLACECURLRIGHT)

def tojsonquotes(strFromPost):
    # Turn single into double quotes
    return strFromPost.replace("\'", "\"")

def fromjsonquotes(strFromPost):
    # Turn double into single quotes.  Won't use it much
    # here, but the remote jobs that feed this will
    return strFromPost.replace("\"", "\'")

def kludgequote(strFromPost):
    return strFromPost.replace("\\\"", "\"")

def singletodouble(stringTo):
    return stringTo.replace('\'', '\"')
####
# dummy for now
def securityCheck():
    return True

####
# Utility for massaging database return info
def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))


#### Define what "app" is
app = Flask(__name__)
app.config.from_object(Config)
##init_db()


####
# Database-related routines
# Note that sqlite3 doesn't like using the same connection
# in different threads
def get_db():
    db = Flask._database = sqlite3.connect(DATABASE)
    db.row_factory = make_dicts
    return db

####
# Historical interest only
def init_db():
    #with app.app_context():
    #    db = get_db()
    #    # I already initialized it 17-Sep-2019
    #    #with app.open_resource('schema.sql', mode='r') as f:
    #    #    db.cursor().executescript(f.read())
    #    #db.commit()
    #    #db.close()
    return

####
# Close connection when done
@app.teardown_appcontext
def close_connection(exception):
    if DEBUGDB:
        print("Do close_connection", str(exception))
    db = getattr(Flask, '_database', None)
    if db is not None:
        if DEBUGDB:
            print('DEBUG: CLOSING db')
        db.close()
    else:
        if DEBUGDB:
            print('DEBUG: NO NEED to close db')

####
# Open a query to the DB.  Do not close the connection
def query_db(query, args=(), one=False):
    if DEBUGDB:
        print("Do query_db")
    conn = get_db()
    try:
        cur = conn.execute(query, args)
    except sqlite3.Error as e:
        conn.close()
        return 'FAILURE ' + str(e.args[0])
    rv = cur.fetchall()
    cur.close()
    #conn.close()
    return (rv[0] if rv else None) if one else rv

####
# Open a query to the DB.  Close the connection
def query_db_final(query, args=(), one=False):
    if DEBUGDB:
        print("Do query_db_final")
    conn = get_db()
    try:
        cur = conn.execute(query, args)
    except sqlite3.Error as e:
        conn.close()
        return 'FAILURE ' + str(e.args[0])
    rv = cur.fetchall()
    cur.close()
    conn.close()
    return (rv[0] if rv else None) if one else rv

####
# Open the DB and insert or update it.  Do not close the connection
def insert_db(query, args=(), one=False):
    if DEBUGDB:
        print("Do insert_db")
    conn = get_db()
    try:
        cur = conn.execute(query, args)
    except sqlite3.Error as e:
        return 'FAILURE ' + str(e.args[0])
    rv = cur.fetchall()
    conn.commit()
    cur.close()
    #conn.close()
    return (rv[0] if rv else None) if one else rv

####
# Insert a row into the DB or update it.  Close the connection
def insert_db_final(query, args=(), one=False):
    if DEBUGDB:
        print("Do insert_db_final")
    conn = get_db()
    try:
        cur = conn.execute(query, args)
    except sqlite3.Error as e:
        return 'FAILURE ' + str(e.args[0])
    rv = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return (rv[0] if rv else None) if one else rv

#############################
#To pass variable parts to the SQL statement,
# use a question mark in the statement and pass
# in the arguments as a list.
#   Never directly add them to the SQL statement
# with string formatting because this makes it
# possible to attack the application using SQL Injections.
# I should probably heed that advice.
#  Revisit these things when I get the basics working correctly


####
# Define operations depending on the URL

####
# Basic null stuff.  2 URLs that do the same thing
@app.route("/")
@app.route("/index")
def index():
    return "Lasciate ogne speranza, voi ch\'intrate."


### The control methods add new rows to or update old rows
###    in database tables, except for the info ones


## dumpcontrol

# Info methods.  2 ways to do the same thing
@app.route("/dumpcontrol")
@app.route("/dumpcontrol/info", methods=["GET", "POST"])
def dumpcontrolinfo():
    stuff = query_db_final('SELECT * FROM DumpCandC ORDER BY dumpCandC_id DESC LIMIT 1')
    if len(stuff) <= 0:
        return ''
    return stuff[0]

# "Update" methods
# These insert a new row, so they want the old status info.
# If there's an sql command to take the most recent row, copy stuff
#  out of it, and insert a new row using that info, I should look at
#  using that instead.  For such a simple DB, it probably is no better.

# Insert a row changing the status
@app.route("/dumpcontrol/update/status/<estring>", methods=["POST"])
def updatedumpstatus(estring):
    # Sanity check--is this one of the approved status possibilities?
    if estring not in DUMPSTATI:
        return 'Must be one of ' + str(DUMPSTATI)
    getallstr = 'SELECT bundlePoolSize,bundleError FROM DumpCandC ORDER BY dumpCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status)'
    if str(estring) == 'Run':
        newstr = newstr + ' VALUES (\'\',' + str(stuff[0]['bundlePoolSize']) + ','  # Clear the error
    else:
        newstr = newstr + ' VALUES (\'' + stuff[0]['bundleError'] + '\',' + str(stuff[0]['bundlePoolSize']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + estring + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

####
# Insert a row updating the local pool size (should be quite large).
#  Also serves as heartbeat for the local scanning system.
@app.route("/dumpcontrol/update/poolsize/<estring>", methods=["POST"])
def updatedumppoolsize(estring):
    try:
        newint = int(estring)	# This sanitizes the input for me
    except:
        return 'Not an integer'
    getallstr = 'SELECT bundleError,status FROM DumpCandC ORDER BY dumpCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + str(stuff[0]['bundleError']) + '\',' + str(newint)
    newstr = newstr + ',datetime(\'now\',\'localtime\'),\'' + str(stuff[0]['status']) + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

####
# Insert a row in the local dump control with an error message
# Whether we change the error or not depends on the message
#
# Not really applicable to the current configuration, in
# which the dumps are done manually and the bundling is
# also done manually via jade scripts.

@app.route("/dumpcontrol/update/bundleerror/<estring>", methods=["POST"])
def updatedumpbundleerror(estring):
    #
    revisedstring = unmangle(estring)
    getallstr = 'SELECT bundlePoolSize,status FROM DumpCandC ORDER BY dumpCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    if len(revisedstring) > 0:
        # Error--change status automatically
        berror = 'Error'
    else:
        berror = str(stuff[0]['status'])   # If clearing it, leave the status the same
    #newstr = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status)'
    #newstr = newstr + ' VALUES (\'' + revisedstring + '\',' + str(stuff[0]['bundlePoolSize']) + ','
    #newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + berror + '\')'
    nnewstr = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status)' + ' VALUES(?, ?, datetime(\'now\',\'localtime\'), ?)'
    params = (revisedstring, str(stuff[0]['bundlePoolSize']), berror)
    #stuff = insert_db_final(newstr)
    stuff = insert_db_final(nnewstr, params)
    # Put in sanity checking
    return 'OK'

####
# Insert a row to reset the dump control to Run, clearing errors
@app.route("/dumpcontrol/update/reset", methods=["POST"])
def updatedumpreset():
    #
    stuff = query_db('SELECT bundlePoolSize from DumpCandC order by dumpCandC_id DESC LIMIT 1')
    bps = str(stuff[0]['bundlePoolSize'])
    #updatestring = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status) \
    #    VALUES ("",' + bps + ',datetime(\'now\',\'localtime\'),"Run")'
    updatestring = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status) \
        VALUES ("",?,datetime(\'now\',\'localtime\'),"Run")'
    params = (bps, )
    stuff = insert_db_final(updatestring, params)
    # No outside input strings, so we should be ok
    return 'OK'


##############

## nersccontrol

# Info methods; 3 URLs to do the same thing
@app.route("/nersccontrol")
@app.route("/nersccontrol/info", methods=["GET", "POST"])
@app.route("/nersccontrol/info/", methods=["GET", "POST"])
def nersccontrolinfo():
    stuff = query_db('SELECT * FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1')
    if len(stuff) <= 0:
        return ''
    return stuff[0]

# Update methods

# Insert a row to change the status of the NERSC loading control
@app.route("/nersccontrol/update/status/<estring>", methods=["POST"])
def updatenerscstatus(estring):
    #
    if estring not in NERSCSTATI:
        return 'Must be one of ' + str(NERSCSTATI)	# This sanitizes the input for me
    #
    getallstr = 'SELECT nerscSize,localError,hpsstree FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO NERSCandC (localError,nerscSize,hpsstree,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + str(stuff[0]['localError']) + '\',' + str(stuff[0]['nerscSize']) + ','
    newstr = newstr + str(stuff[0]['hpsstree']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + estring + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking?
    return 'OK'

# Insert a row to update the NERSC pool size.  This also serves as a heartbeat
# for the NERSC loading control
@app.route("/nersccontrol/update/poolsize/<estring>", methods=["POST"])
def updatenerscpoolsize(estring):
    #
    #print('update/poolsize/', estring)
    securityCheck()
    try:
        newint = int(estring)	# This sanitizes the input for me
    except:
        return 'Not an integer'
    getallstr = 'SELECT localError,nerscError,status,hpsstree FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO NERSCandC (localError,nerscError,nerscSize,hpsstree,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + str(stuff[0]['localError']) + '\',\'' + stuff[0]['nerscError'] + '\','
    newstr = newstr + str(newint) + ',' + str(stuff[0]['hpsstree'])
    newstr = newstr + ',datetime(\'now\',\'localtime\'),\'' + str(stuff[0]['status']) + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

# Insert a row to update the NERSC loader control error message.  Changes the
# status if appropriate
@app.route("/nersccontrol/update/nerscerror/<estring>", methods=["POST"])
def updatenerscerror(estring):
    #
    revisedstring = unmangle(estring)
    getallstr = 'SELECT nerscSize,localError,status,hpsstree FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    if len(revisedstring) > 0:
        # Error--change status automatically
        berror = 'Error'
    else:
        berror = str(stuff[0]['status'])   # If clearing it, leave the status the same
    nnewstr = 'INSERT INTO NERSCandC (nerscError,localError,nerscSize,hpsstree,lastChangeTime,status)'
    nnewstr = nnewstr + ' VALUES(?, ?, ?, ?, datetime(\'now\',\'localtime\'), ?)'
    params = (revisedstring, str(stuff[0]['localError']), str(stuff[0]['nerscSize']), str(stuff[0]['hpsstree']), berror)
    if DEBUGDB:
        print(nnewstr)
    stuff = insert_db_final(nnewstr, params)
    if DEBUGDB:
        print(stuff)
    # Put in sanity checking
    return 'OK'

# Insert a row to clear the NERSC loader control error message.  Changes the
# status if appropriate
@app.route("/nersccontrol/update/nerscerror/clear", methods=["POST"])
def resetnerscerror():
    #  All internal, so should be safe
    getallstr = 'SELECT nerscSize,localError,status,hpsstree FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    berror = str(stuff[0]['status'])   # If clearing it, leave the status the same
    newstr = 'INSERT INTO NERSCandC (nerscError,localError,nerscSize,hpsstree,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'\',\'' + str(stuff[0]['localError']) + '\','
    newstr = newstr + str(stuff[0]['nerscSize']) + ',' + str(stuff[0]['hpsstree']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + berror + '\')'
    if DEBUGDB:
        print(newstr)
    stuff = insert_db_final(newstr)
    if DEBUGDB:
        print(stuff)
    # Put in sanity checking
    return 'OK'

# Insert a row updating the "localerror" for NERSC control.  This is
# probably going to refer to the globus transfer errors, if it is used
# at all.  I'm wavering on it.
@app.route("/nersccontrol/update/localerror/<estring>", methods=["POST"])
def updatenersclocalerror(estring):
    #
    revisedstring = unmangle(estring)
    getallstr = 'SELECT nerscSize,nerscError,status,hpsstree FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    if len(revisedstring) > 0:
        # Error--change status automatically
        berror = 'Error'
    else:
        berror = str(stuff[0]['status'])   # If clearing it, leave the status the same
    newstr = 'INSERT INTO NERSCandC (localError,nerscError,nerscSize,lastChangeTime,status)\
        VALUES (?,?,?,?,datetime(\'now\',\'localtime\'),?)'
    params = (revisedstring, str(stuff[0]['nerscError']), str(stuff[0]['nerscSize']), str(stuff[0]['hpsstree']), berror)
    stuff = insert_db_final(newstr, params)
    # Put in sanity checking
    return 'OK'

# Insert a row setting the NERSC control to Run, clearing the error
@app.route("/nersccontrol/update/reset", methods=["POST"])
def updatenerscreset():
    #
    stuff = query_db('SELECT nerscSize,hpsstree from NERSCandC order by nerscCandC_id DESC LIMIT 1')
    updatestring = 'INSERT INTO NERSCandC (localError,nerscError,nerscSize,lastChangeTime,status) \
        VALUES ("","",?,?,datetime(\'now\',\'localtime\'),"Run")'
    params = (str(stuff[0]['nerscSize']), str(stuff[0]['hpsstree']))
    stuff = insert_db_final(updatestring, params)
    # Put in sanity checking
    return 'OK'



### Heartbeat methods

@app.route("/nerscheartbeat/<estring>", methods=["GET", "POST"])
def nerscheartbeat(estring):
    params = (estring,)
    if request.method == 'GET':
        sstring = 'SELECT * FROM Heartbeats where hostname=?'
        stuff = query_db_final(sstring, params)
        #stuff = query_db('SELECT * FROM Heartbeats where hostname=\"' + estring + '\"')
        if len(stuff) == 0:
            return "404 Not Found"
        return stuff[0]    # Let the remote user figure it out.  Keep it simple
    sstring = 'UPDATE Heartbeats set lastChangeTime=datetime(\'now\',\'localtime\') WHERE hostname=\"?\"'
    stuff = insert_db_final(sstring, params)
    if len(stuff) == 0:
        return ""
    return stuff[0]

### Token methods

# Take the token--if not already in use
@app.route("/nersctokentake/<estring>", methods=["POST"])
def nersctake(estring):
    answer = query_db('SELECT hostname FROM Token')
    if len(answer) > 1:
        print(len(answer), str(answer), 'BAD ERROR')
        # THIS IS A BAD ERROR, SHOULD NEVER HAPPEN
        status = "BUSY"
    else:
        # 2 Cases:  Case 1, the string is blank.  Set it (take token), return OK
        #  Case 2, the string is not blank.  Return BUSY
        #fjson = json.loads(unmangle(answer[0]).unquote_plus(estring))
        fjson = answer[0]  #json.loads(singletodouble(answer[0]))
        #if str(answer[0]) == '':
        if fjson['hostname'] == '':
            params = (estring,)
            string = 'UPDATE Token SET hostname=?,lastChangeTime=datetime(\'now\',\'localtime\')'
            stuff = insert_db_final(string, params)
            if len(stuff) > 1:
                # What went wrong?
                print(str(stuff))
                status = "BUSY"
            else:
                status = "OK"
        else:
            # Somebody else (maybe another process on the same system?)
            # has the token
            #print('Token reply=', str(answer))
            status = "BUSY"
    return status

# Release the token
#@app.route("/nersctokenrelease", methods=["POST"])
@app.route("/nersctokenrelease/", methods=["POST"])
def nerscrelease():
    answer = insert_db_final('UPDATE Token SET hostname=\'\',lastChangeTime=datetime(\'now\',\'localtime\')')
    if len(answer) > 1:
        if DEBUGDB:
            print(len(answer), str(answer), 'DID IT RELEASE?')
        return "BUSY"
    return "OK"

# Is the token in use?  (Monitoring)
@app.route("/nersctokeninfo", methods=["GET"])
def tokenusage():
    answer = query_db('SELECT hostname,lastChangeTime FROM Token')
    #print(answer)
    if len(answer) < 1:
        return "SILENT"
    return answer[0]

# What are the heartbeats? [Monitoring]
@app.route("/heartbeatinfo/", methods=["GET"])
def senseheartbeats():
    answer = query_db('SELECT hostname,lastChangeTime FROM Heartbeats')
    #print(answer)
    if len(answer) < 1:
        return ""
    return str(answer)

#####
#

### Bundle methods

# Get the oldest untouched bundle.  I.E. -- the next one to start processing
#@app.route("/bundles/oldestuntouched", methods=["GET"])
@app.route("/bundles/oldestuntouched/", methods=["GET"])
def getoldestuntouched():
    qstring = 'SELECT * FROM BundleStatus WHERE status=\"Untouched\" ORDER BY bundleStatus_id ASC LIMIT 1'
    stuff = query_db_final(qstring)
    # sanity checking?
    if 'DOCTYPE HTML PUBLIC' in stuff[0]:
        return ['FAILURE']
    return str(stuff)


# Get the untouched bundles, in order of decreasing age
#@app.route("/bundles/alluntouched", methods=["GET"])
@app.route("/bundles/alluntouched/", methods=["GET"])
def getalluntouched():
    qstring = 'SELECT * FROM BundleStatus WHERE status=\"Untouched\" ORDER BY bundleStatus_id ASC'
    stuff = query_db_final(qstring)
    # sanity checking?
    if len(stuff) <= 0:
        return ''
    if 'DOCTYPE HTML PUBLIC' in stuff[0]:
        return ['FAILURE']
    return str(stuff)

# Get the bundle satisfying certain criteria.
# The default is the oldest untouched, but you can specify anything,
# and may get all of them if that's your pleasure
@app.route("/bundles/specified/<estring>", methods=["GET"])
def getspecified(estring):
    if not estring:
        if DEBUGDB:
            print('getspecified, no estring')
        qstring = 'SELECT * FROM BundleStatus WHERE status=\"Untouched\" ORDER BY bundleStatus_id ASC LIMIT 1'
        stuff = query_db_final(qstring)
        # sanity checking?
        return stuff[0]
    unstring = kludgequote(unmangle(estring))
    params = (unstring,)
    if DEBUGDB:
        print('getspecified', unstring)
    qstring = 'SELECT * FROM BundleStatus WHERE status = ?'
    try:
        stuff = query_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ""
    except:
        print('getspecified problem:', qstring, params, str(stuff))
        return ""


@app.route("/bundles/specifiedin/<estring>", methods=["GET"])
def getspecifiedin(estring):
    if  not estring:
        return []
    unstring = kludgequote(unmangle(estring))
    words = unstring.split(',')
    if len(words) == 1:
        params = (unstring,)
    else:
        params = (words)
    if DEBUGDB:
        print('getspecified', unstring)
    qstring = 'SELECT * FROM BundleStatus WHERE status!=\"Abort\" AND localname IN ('
    for i in range(len(words)):
        qstring = qstring + '?,'
    qqstring = qstring[::-1].replace(',', ')', 1)[::-1]
    try:
        stuff = query_db_final(qqstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ""
    except:
        print('getspecified problem:', qstring, params, str(stuff))
        return ""

# Get the specified bundle.  The spec'ed id can be file name or id
@app.route("/bundles/get/<estring>", methods=["GET"])
def bundleget(estring):
    if not estring:
        return ""
    unstring = kludgequote(unmangle(estring))
    try:
        bid = int(unstring)
        # If ok, we have an integer, use it as bundleStatus_id
        qstring = 'SELECT * FROM BundleStatus WHERE bundleStatus_id=?'
        params = (str(bid), )
    except:
        # OK, must be a file name
        qstring = 'SELECT * FROM BundleStatus where localName LIKE ?'
        params = ('%' + unstring +'%', )
    try:
        stuff = query_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ''
    except:
        print('bundleget failed:', unstring, stuff)
        return ''

# Get the bundles like the string, with optional status as well
@app.route("/bundles/getlike/<estring>", methods=["GET"])
def bundlegetlike(estring):
    if not estring:
        return ""
    unstring = kludgequote(unmangle(estring))
    # ASSUMING NO SPACES IN FILE NAMES!!
    wstring = unstring.split()
    if len(wstring) == 1:
        qstring = 'SELECT * FROM BundleStatus WHERE idealName LIKE ?'
        params = ('%' + unstring + '%', )
    else:
        if len(wstring) == 2:
            qstring = 'SELECT * FROM BundleStatus WHERE idealName LIKE ? AND status = ?'
            params = ('%' + wstring[0] + '%', wstring[1])
        else:
            print('bundlegetlike: Bad number of arguments', unstring)
            return 'FAILURE'
    #
    try:
        stuff = query_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ''
    except:
        print('bundlesgetlike failed:', unstring, stuff)
        return ''

# Patch the specified bundle.  String format is 'id:type:value'
@app.route("/bundles/patch/<estring>", methods=["POST"])
def bundlepatch(estring):
    if not estring:
        return 'FAILURE'
    unstring = kludgequote(unmangle(estring))
    words = unstring.split(':')
    chunks = len(words)
    residual = chunks - 2 * int(chunks/2)
    if chunks < 3 or residual != 1:
        print('bundlepatch:', unstring)
        return 'FAILURE'
    #
    try:
        bid = int(words[0])
        bkey = 'bundleStatus_id=? '
        param2 = words[0]
    except:
        bkey = 'localName LIKE ? '
        param2 = '%' + words[0] + '%'
    #
    if words[1] not in BUNDLECOLS:
        print('bundlepatch bad argument', words)
        return 'FAILURE'
    #
    qstring = 'UPDATE BundleStatus SET ' + words[1] + '=? WHERE ' + bkey
    params = (words[2], param2)
    try:
        stuff = insert_db_final(qstring, params)
        if len(str(stuff)) > 0:
            if str(stuff) != '[]':
                return 'FAILURE ' + str(stuff) + ' ' + qstring + str(params)
    except:
        print('bundlepatch problem:', qstring, params, str(stuff))
        return "FAILURE"
    return "OK"

# Update the specified bundle with new status and jade uuid
@app.route("/updatebundle/statusuuid/<estring>", methods=["POST"])
def updatebundlestatusuuid(estring):
    unstring = kludgequote(unmangle(estring))
    words = unstring.split()
    if len(words) != 3:
        print('updatebundlestatusuuid got', words)
        return 'FAILURE'
    qstring = 'UPDATE BundleStatus SET status=?,UUIDJade=? WHERE bundleStatus_id=?'
    params = (words[0], words[1], words[2])
    try:
        stuff = insert_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return 'FAILURE ' + str(stuff)
        return ""
    except:
        print('updatebundlestatusuuid problem:', qstring, params, str(stuff))
        return "FAILURE"

# Get information about the specified bundle, knowing the UUIDJade name
@app.route("/bundles/infobyjade/<estring>", methods=["GET"])
def getinfobyjade(estring):
    unstring = kludgequote(unmangle(estring))
    words = unstring.split()
    if len(words) == 1:
        qstring = 'SELECT bundleStatus_id,idealName,status FROM BundleStatus WHERE UUIDJade=?'
        params = (words[0],)
    else:
        qstring = 'SELECT bundleStatus_id,idealName,status FROM BundleStatus WHERE UUIDJade=? AND status=?'
        params = (words[0], words[1])
    try:
        stuff = query_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ""
    except:
        print('getinfobyjade problem:', qstring, params, str(stuff))
        return ""

# Get all the bundles associated with the specified string.  This
# is useful for getting all bundles for a given directory
@app.route("/bundles/specifiedlike/<estring>", methods=["GET"])
def getspecifiedlike(estring):
    unstring = kludgequote(unmangle(estring))
    qstring = 'SELECT * FROM BundleStatus WHERE idealName LIKE ?'
    params = ('%' + unstring + '%', )
    try:
        stuff = query_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ""
    except:
        print('getspecifiedlike problem:', qstring, params, str(stuff))
        return ""


# Get the count of the bundle status type specified
@app.route("/bundles/statuscount/<estring>", methods=["GET"])
def getstatuscount(estring):
    unstring = str(kludgequote(unmangle(estring)))
    if unstring not in BUNDLESTATI:
        return 'Invalid status: ' + unstring
    qstring = 'SELECT COUNT(*) FROM BundleStatus WHERE status=?'
    params = (unstring,)
    try:
        stuff = query_db_final(qstring, params)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ""
    except:
        print('getstatuscount problem:', qstring, params, str(stuff))
        return ""

# Get partial info about all bundles not done or abandoned
@app.route("/bundles/working", methods=["GET"])
def getworking():
    qstring = 'SELECT status,localName,bundleStatus_id FROM BundleStatus WHERE status NOT IN (\'Abort\',\'LocalDeleted\')'
    try:
        stuff = query_db_final(qstring)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ""
    except:
        print('getworking problem:', qstring, str(stuff))
        return ""


# Update the specified bundle.  Wants the bundleStatus_id
@app.route("/updatebundle/<estring>", methods=["GET", "POST"])
def updatebundle(estring):
    # Yes, of course.  I use the string as the command.  All sorts
    # of horrors are possible
    unstring = kludgequote(unmangle(estring))
    try:
        stuff = insert_db_final(unstring)
        if len(stuff) > 0:
            return str(stuff)
        return ""
    except:
        print("updatebundle Failed with", unstring)
        return ""

# Update the specified bundle's status.  Needs the bundle_status_id and the status
@app.route("/updatebundle/status/<estring>", methods=["POST"])
def updatebundlestatus(estring):
    # estring format is 'newstatus key' separated by a space
    unstring = kludgequote(unmangle(estring))
    words = unstring.split()
    if len(words) != 2:
        return 'FAILURE: too many arguments'
    qstring = 'UPDATE BundleStatus SET status=? WHERE bundleStatus_id=?'
    qparams = (words[0], words[1])
    try:
        stuff = insert_db_final(qstring, qparams)
        if len(stuff) > 0:
            return str(stuff)
        return ''
    except:
        print('updatebundlestatus Failed with', qstring, qparams)
        return 'FAILURE'


###
# Get all the bundle info in one swell foop
@app.route("/bundles/allbundleinfo", methods=["GET"])
def getallbundleinfo():
    query = 'SELECT COUNT(*) AS total'
    for stat in BUNDLESTATI:
        query = query + ', SUM(CASE WHEN status=\"' + stat + '\" THEN 1 ELSE 0 END) AS ' + stat
    query = query + ' FROM BundleStatus'
    #
    try:
        stuff = query_db_final(query)
        if len(str(stuff)) > 0:
            return str(stuff)
        return ''
    except:
        print('getallbundleinfo problem:', query, str(stuff))
        return ''

###
#ActiveDirectory (idealDir TEXT PRIMARY KEY, lastChangeTime TEXT);
@app.route("/bundles/gactive/add/<estring>", methods=["POST"])
def activediradd(estring):
    ''' Add the directory to ActiveDirectory table: timestamp for debugging '''
    newdir = urllib.parse.unquote_plus(unmangle(reslash(estring)).replace('\'', '\"'))
    query = 'INSERT INTO ActiveDirectory (idealDir, lastChangeTime) VALUES '
    query = query + '(?, datetime(\'now\', \'localtime\')'
    params = (newdir, )
    try:
        stuff = insert_db_final(query, params)
    except:
        # Might already exist
        print('activediradd:  did not add', newdir)
        return str(stuff)
    return ''

@app.route("/bundles/gactive/remove/<estring>", methods=["POST"])
def activedirremove(estring):
    ''' remove the directory from ActiveDirectory '''
    newdir = urllib.parse.unquote_plus(unmangle(reslash(estring)).replace('\'', '\"'))
    query = 'DELETE FROM ActiveDirectory WHERE idealDir=?'
    params = (newdir, )
    try:
        stuff = insert_db_final(query, params)
    except:
        # Might not exist:  bad name or already done
        print('activedirremove:  did not remove', newdir)
        return str(stuff)
    return ''


@app.route("/bundles/gactive/find/<estring>", methods=["GET"])
def activedirfind(estring):
    ''' Return the information about this directory.  It need not exist '''
    newdir = urllib.parse.unquote_plus(unmangle(reslash(estring)).replace('\'', '\"'))
    query = 'SELECT FROM ActiveDirectory WHERE idealDir LIKE ?'
    params = ('%' + newdir + '%', )
    try:
        stuff = query_db_final(query, params)
    except:
        print('activedirfind: failed to execute', query, params, stuff)
        return ''
    # Might not exist.  That is not an error.
    return str(stuff)

@app.route("/bundles/gactive/clean", methods=["POST"])
def activedirclean():
    ''' Clean out completed ActiveDirectory entries '''
    # When a bundle arrives at NERSC its status changes from
    # JsonMade to something else
    # Provided that isn't a NERSCProblem, that means done.
    # If all the Bundle's associated with this directory have
    # status other than JsonMade or NERSCProblem, delete the row
    query = 'SELECT idealDir FROM ActiveDirectory'
    try:
        stuff = query_db_final(query)
    except:
        print('activedirclean: initial query failed', stuff)
        return 'FAILURE'
    if len(stuff) < 2:
        return ''    # nothing to do
    for ad_row in stuff:
        #
        idealDir = str(ad_row['idealDir'])
        query2 = 'SELECT bundleStatus_id,idealName,status FROM BundleStatus '
        query2 = query2 + 'WHERE idealName LIKE ?'
        params2 = ('%' + idealDir + '%', )
        try:
            bstuff = query_db_final(query2, params2)
        except:
            print('activedirclean: bundle query failed', query2, params2, bstuff)
            return 'FAILURE 2'
        count = 0
        for bun_row in bstuff:
            #
            if os.path.dirname(str(bun_row['idealName'])) != idealDir:
                continue
            if bun_row['status'] in ['JsonMade', 'NERSCProblem']:
                count = count + 1
        if count == 0:
            query3 = 'DELETE FROM ActiveDirectory WHERE idealDir=?'
            params3 = (ad_row, )
            try:
                dstuff = insert_db_final(query3, params3)
            except:
                print('activedirclean: row delete failed', query3, params3, dstuff)
        #
    return ''

### Insertion methods for bundles.  These aren't updates, but new bundles
# This needs some debugging yet.
@app.route("/addbundle/<estring>", methods=["POST"])
def addbundle(estring):
    ''' Add bundles '''
    # Above is for 28-Jan-2020.
    backagain = urllib.parse.unquote_plus(unmangle(reslash(estring)).replace('\'', '\"'))
    #print(backagain)
    # crazy hack--getting rid of those excess backslashes is a pain
    try:
        fjson = (json.loads(backagain[1:-2] + '}'))
    except:
        print('addbundle', backagain)
        return "Not valid json"
    #
    #print(type(fjson))
    #print(fjson)
    #for x in fjson:
    #    print(x)
    try:
        lname = str(fjson['localName'])
    except:
        return "json does not include localName"
    # BTW, check that BUNDLESTATUSCOLUMS is correct.
    # It's ok as of 28-Jan-2020, but if the schema changes...
    params = (lname,)
    qstring = 'SELECT * FROM BundleStatus WHERE localName=?'
    try:
        stuff = query_db(qstring, params)
        #print(stuff)
    except:
        print("addbundle: Some kind of error")
        return "BAD"
    if len(stuff) > 1:
        try:
            testid = stuff[0]['bundleStatus_id']
            nogo = True
        except:
            nogo = False
        if nogo:
            print("addbundle: No: Already exists")
            return "already exists, insert forbidden"
    initialstring = "INSERT INTO BundleStatus (localName,idealName,size,checksum,UUIDJade,UUIDGlobus,useCount,status,LooseFileDir,DownloadDir,FileDate) VALUES"
    # Sanity check
    for inargs in fjson:
        if not inargs in BUNDLESTATUSCOLUMNS:
            return inargs + " is not a valid database column for BundleStatus"
    idealName = str(fjson['idealName'])
    initialstring = initialstring + "(?,?,?,?,\"\",\"\",1,\"Untouched\",\"\",\"\",datetime(\"now\",\"localtime\"))"
    params = (str(fjson['localName']), idealName, str(fjson['size']), str(fjson['checksum']))
    #print(initialstring)
    stuff = insert_db_final(initialstring, params)
    if len(stuff) > 1:
        #What went wrong?
        print('addbundle', str(stuff))
        return 'FAILURE? ' + str(stuff)
    return 'OK done'

# Get info about which data warehouse trees to search
@app.route("/tree/<estring>", methods=["GET"])
def gettree(estring):
    # Only load this database table manually
    params = (urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'),)
    initialstring = "SELECT treetop FROM Trees WHERE ultimate=?"
    try:
        stuff = query_db_final(initialstring, params)
    except:
        print("gettree: Failed to query")
        stuff = ''
    return str(stuff)


###
### Dump control
###

###
# Return the current state of the DumpSystemState
@app.route("/dumping/state", methods=["GET"])
def dumpstate():
    #
    query = "SELECT * FROM DumpSystemState ORDER BY dumpss_id DESC LIMIT 1"
    try:
        stuff = query_db_final(query)
    except:
        print("dumpstate:  Failed to query")
        stuff = ''
    return stuff[0]


###
# Adjust the current state of the DumpSystemState
# Adjust the current state of the DumpSystemState
@app.route("/dumping/state/count/<estring>", methods=["POST"])
def dumpstatesetcount(estring):
    return dumpstateset('count', estring)
@app.route("/dumping/state/nextaction/<estring>", methods=["POST"])
def dumpstatesetnextaction(estring):
    return dumpstateset('nextaction', estring)
@app.route("/dumping/state/status/<estring>", methods=["POST"])
def dumpstatesetstatus(estring):
    return dumpstateset('status', estring)
#
# Utility
def dumpstateset(toupdate, estring):
    #
    astring = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'),)
    # sanity checking
    if toupdate not in ['count', 'nextaction', 'status']:
        print('dumpstateset:  action not allowed', toupdate)
        return 'FAILURE'
    if toupdate == 'count':
        try:
            icount = int(astring)
        except:
            print('dumpstateset: count to be set to non-integer', estring)
            return 'FAILURE'
    if toupdate == 'nextaction':
        if astring not in DumperNextOptions:
            print('dumpstateset:', astring, 'not a valid nextaction')
            return 'FAILURE'
    if toupdate == 'status':
        if astring not in DumperStatusOptions:
            print('dumpstateset:', astring, 'not a valid state')
            return 'FAILURE'
    # SANE!
    #
    query1 = "SELECT * FROM DumpSystemState ORDER BY dumpss_id DESC LIMIT 1"
    query2 = "INSERT INTO DumpSystemState (nextAction, status, lastChangeTime, count)"
    query2 = query2 + " VALUES (?,?,datetime(\'now\',\'localtime\'),?)"
    #
    try:
        stuff = query_db_final(query1)
    except:
        print("dumpstateset:  Failed to query1")
        return 'FAILURE'
    try:
        nexta = str(stuff[0]['nextAction'])
        status = str(stuff[0]['status'])
        count = str(stuff[0]['count'])
    except:
        print('dumpstateset, failed to unpack dictionary', stuff)
        return 'FAILURE'
    #
    if toupdate == 'count':
        count = astring
    if toupdate == 'nextaction':
        nexta = astring
    if toupdate == 'status':
        status = astring
    params = (nexta, status, count)
    try:
        stuff = insert_db_final(query2, params)
    except:
        print('dumpstateset:  Failed to update query2', query2, params)
        return 'FAILURE'
    return ''


###
# PoleDisk manipulations
#
# Get all the info on all the PoleDisks.  A lot!
@app.route("/dumping/poledisk", methods=["GET"])
def polediskinfoall():
    query = 'SELECT * FROM PoleDisk ORDER BY poledisk_id ASC'
    try:
        stuff = query_db_final(query)
    except:
        print('polediskinfo:  Failed to read')
        stuff = ''
    return str(stuff)

# Get all the info on the PoleDisks that used this slot and aren't
# Done yet.  May be some 'Error' in here, => may be more than 1
@app.route("/dumping/poledisk/infobyslot/<estring>", methods=["GET"])
def polediskinfobyslot(estring):
    query = 'SELECT * FROM PoleDisk WHERE slotnumber=? AND status != \'Done\' ORDER BY poledisk_id DESC'
    unstring = unmangle(estring)
    try:
        inum = int(unstring)
    except:
        print('polediskinfobyslot:  invalid slot number (1-12)', estring)
        return ''
    if inum < 1 or inum > 12:
        print('polediskinfobyslot:  slot number out of range (1-12)', inum)
        return ''
    params = (unstring, )
    try:
        stuff = query_db_final(query, params)
    except:
        print('polediskinfobyslot: query failed', query, params)
        stuff = ''
    return str(stuff)

# Get all the info on the PoleDisks of this UUID and aren't
# Done yet.  May be some 'Error' in here, => may be more than 1
@app.route("/dumping/poledisk/infobyuuid/<estring>", methods=["GET"])
def polediskinfobyuuid(estring):
    query = 'SELECT * FROM PoleDisk WHERE diskuuid=? AND status != \'Done\' ORDER BY poledisk_id DESC'
    unstring = unmangle(estring)
    params = (unstring, )
    try:
        stuff = query_db_final(query, params)
    except:
        print('polediskinfobyuuid: query failed', query, params)
        stuff = ''
    return str(stuff)

# Get all the info on the PoleDisk with this poledisk_id
@app.route("/dumping/poledisk/infobyid/<estring>", methods=["GET"])
def polediskinfobyid(estring):
    query = 'SELECT * FROM PoleDisk WHERE poledisk_id=?'
    unstring = unmangle(estring)
    params = (unstring, )
    try:
        stuff = query_db_final(query, params)
    except:
        print('polediskinfobyid: query failed', query, params)
        stuff = ''
    return str(stuff)

# Set the start time of the dump.  Wants the poledisk_id as a parameter
@app.route("/dumping/poledisk/start/<estring>", methods=["POST"])
def polediskstart(estring):
    query = 'UPDATE PoleDisk SET dateBegun=datetime(\'now\',\'localtime\'),status=\'Dumping\' WHERE poledisk_id=?'
    unstring = unmangle(estring)
    try:
        inum = int(unstring)
    except:
        print('polediskstart:  invalid poledisk_id', estring)
        return 'FAILURE'
    params = (unstring, )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('polediskstart:  Failed update', query, params)
        return 'FAILURE'
    if len(stuff) > 0:
        print('polediskstart:  Failed update', query, params, stuff)
        return 'FAILURE'
    return ''

# Set the status of the PoleDisk.  Wants poledisk_id and status
@app.route("/dumping/poledisk/setstatus/<estring>", methods=["POST"])
def poledisksetstatus(estring):
    # unpack estring first
    unstring = unmangle(estring).split()
    if len(unstring) != 2:
        print('poledisksetstatus has the wrong number of arguments', unstring)
        return 'FAILURE, number arguments'
    try:
        poleid = int(unstring[0])
    except:
        print('poledisksetstatus cannot read the poledisk_id', estring)
        return 'FAILURE, not integer'
    if unstring[1] not in PoleDiskStatusOptions:
        print('poledisksetstatus has bad status', estring)
        return 'FAILURE, bad status'
    query = 'UPDATE PoleDisk SET status=? WHERE poledisk_id=?'
    params = (unstring[1], poleid)
    try:
        stuff = insert_db_final(query, params)
    except:
        print('poledisksetstatus:  Failed update', query, params)
        return 'FAILURE'
    if len(stuff) > 0:
        print('poledisksetstatus:  Failed update', query, params, stuff)
        return 'FAILURE'
    return ''

# Set the end time of the dump.  Wants the poledisk_id as a parameter
@app.route("/dumping/poledisk/done/<estring>", methods=["POST"])
def polediskdone(estring):
    query = 'UPDATE PoleDisk SET dateEnded=datetime(\'now\',\'localtime\'),status=\'Done\' WHERE poledisk_id=?'
    unstring = unmangle(estring)
    try:
        inum = int(unstring)
    except:
        print('polediskdone:  invalid poledisk_id', estring)
        return 'FAILURE'
    params = (unstring, )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('polediskdone:  Failed update', query, params)
        return 'FAILURE'
    if len(stuff) > 0:
        print('polediskdone:  Failed update', query, params, stuff)
        return 'FAILURE'
    return ''


# Add a new PoleDisk entry
@app.route("/dumping/poledisk/loadfrom/<estring>", methods=["POST"])
def polediskload(estring):
    query = 'INSERT INTO PoleDisk (diskuuid,slotnumber,dateBegun,dateEnded,targetArea,status) VALUES '
    query = query + '(?, ?, \'\', \'\', ?, ?)'
    backagain = unmangle(reslash(estring).replace('\'', '\"'))
    try:
        fjson = json.loads(backagain)
    except:
        print('polediskload: failed to turn into json', estring)
        return 'FAILURE to make json'
    try:
        diskuuid = fjson['diskuuid']
        slotnumber = fjson['slotnumber']
        targetArea = fjson['targetArea']
        status = 'Inventoried'
    except:
        print('polediskload: Cannot get info from', fjson)
        return 'FAILURE to get info'
    params = (diskuuid, slotnumber, targetArea, status)
    try:
        stuff = insert_db_final(query, params)
    except:
        print('polediskload: Failed to load from', query, params)
        return 'FAILURE to load'
    return ''


#####
# Adjust dump-target.  Not something to do lightly or often

# Get info
@app.route("/dumping/dumptarget", methods=["GET"])
def dumptarget():
    query = 'SELECT target from DumpTarget'
    try:
        stuff = query_db_final(query)
    except:
        print('dumptarget get failed')
        return 'FAILURE'
    return str(stuff)

####
# Set dump-target.  Add the current to the list of OldDumpTargets
# if appropriate (target is a PRIMARY KEY, so it should just fail
# if it is old)
@app.route("/dumping/dumptarget/<estring>", methods=["POST"])
def dumptargetset(estring):
    query = 'SELECT target FROM DumpTarget LIMIT 1'
    try:
        stuff = query_db_final(query)
    except:
        print('dumptargetset failed to get target info', query)
        return 'FAILURE'
    #
    query = 'INSERT INTO OldDumpTargets (target) VALUES (?)'
    params = (str(stuff), )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('dumptargetset failed to set oldtarget', query, params)
    # Don't bail above.  Dunno what happens if duplicate primary key, which
    # is harmless
    query = 'UPDATE DumpTarget SET target = ?'
    newdir = unmangle(urllib.parse.unquote_plus(reslash(estring)))
    params = (newdir, )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('dumptargetset failed to set target', query, params)
        return 'FAILURE'
    return ''


####
# Get info from OldDumpTarget
@app.route("/dumping/olddumptarget", methods=["GET"])
def olddumptarget():
    query = 'SELECT target from OldDumpTargets'
    query = 'SELECT target FROM DumpTarget'
    try:
        stuff = query_db_final(query)
    except:
        print('olddumptarget failed to get target info', query)
        return 'FAILURE'
    return '{ \"slots\":' + str(stuff) + '}'

####
# Get the contents of all the slots.  This will point to
# the PoleDisk information.  It hardly seems worth the
# hassle to select info by slot--there are only 12 and
# the info is lightweight
@app.route("/dumping/slotcontents", methods=["GET"])
def getslotcontents():
    query = 'SELECT * FROM SlotContents ORDER BY slotnumber ASC'
    try:
        stuff = query_db_final(query)
    except:
        print('getslotcontents failed to get the slot info', query)
        return 'FAILURE'
    return str(stuff)

####
# Get full information about the populated slots.
@app.route("/dumping/fullslots", methods=["GET"])
def getfullslots():
    ''' Get info from SlotContents and PoleDisk for populated slots '''
    query = 'SELECT sc.slotnumber,sc.poledisk_id,pd.dateBegun,pd.dateEnded,pd.targetArea,pd.status '
    query = query + 'FROM SlotContents AS sc JOIN PoleDisk AS pd ON (sc.poledisk_id=pd.poledisk_id'
    query = query + ' AND sc.poledisk_id>0) ORDER by sc.slotnumber ASC'
    try:
        stuff = query_db_final(query)
    except:
        print('getfulllots failed to get info', query)
        return 'FAILURE'
    return str(stuff)


####
# Get the contents of a slot.  This is just a couple of
# numbers:  'slot# poledisk_id'
@app.route("/dumping/slotcontents/<estring>", methods=["POST"])
def setslot(estring):
    backagain = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    words = backagain.split()
    if len(words) != 2:
        print('setslot wants 2 arguments:  slot# and poledisk_id', estring)
        return 'FAILURE'
    try:
        testint1 = int(words[0])
        testint2 = int(words[1])
    except:
        print('setslot wants integer arguments:  slot# and poledisk_id', estring)
        return 'FAILURE'
    if testint1 < 1 or testint1 > 12 or testint2 < 0:
        print('setslot wants the first argument 1..12, the second poledisk_id', estring)
        return 'FAILURE'
    query = 'UPDATE SlotContents SET poledisk_id = ? WHERE slotnumber = ?'
    params = (words[1], words[0])
    try:
        stuff = insert_db_final(query, params)
    except:
        print('setslot failed to set the slot info', query, params)
        return 'FAILURE'
    return ''

####
# Get the UUIDs for the disks that have active jobs running
@app.route("/dumping/activeslots", methods=["GET"])
def getactiveslotuuid():
    # Get them all
    query = 'select pd.diskuuid,pd.slotnumber,pd.poledisk_id,pd.dateBegun,sc.name from PoleDisk as pd join SlotContents as sc'
    query = query + ' on (pd.poledisk_id>0 and pd.poledisk_id=sc.poledisk_id and pd.status=\'Dumping\')'
    try:
        stuff = query_db_final(query)
    except:
        print('getactiveslotuuid', query)
        return 'FAILURE'
    return str(stuff)

####
# Get the next UUID and slot number for the disks that don't yet have jobs running
@app.route("/dumping/waitingslots", methods=["GET"])
def getwaitingslotuuid():
    # Get them all
    query = 'select pd.diskuuid,pd.slotnumber,pd.poledisk_id from PoleDisk as pd join SlotContents as sc'
    query = query + ' on (sc.poledisk_id>0 and pd.poledisk_id=sc.poledisk_id and pd.status=\'Inventoried\')'
    query = query + ' ORDER BY pd.poledisk_id ASC LIMIT 1'
    try:
        stuff = query_db_final(query)
    except:
        print('getwaitingslotuuid', query)
        return 'FAILURE'
    return str(stuff)

####
# Get the status for all the slot numbers
@app.route("/dumping/whatslots", methods=["GET"])
def getwhatslot():
    # Get them all
    query = 'select pd.slotnumber,pd.status,pd.poledisk_id from PoleDisk as pd join SlotContents as sc'
    query = query + ' on (sc.poledisk_id>0 and pd.poledisk_id=sc.poledisk_id)'
    query = query + ' ORDER BY pd.poledisk_id ASC'
    try:
        stuff = query_db_final(query)
    except:
        print('getwhatslot', query)
        return 'FAILURE'
    return str(stuff)


####
# Get the substring representing the trees we want to retrieve
#  Format is IceCube/YEAR/internal-system/pDAQ-2ndBld, where
# YEAR is to be replaced by the year(s) found on the disk
@app.route("/dumping/wantedtrees", methods=["GET"])
def getwantedtrees():
    query = 'SELECT dataTree from WantedTrees'
    try:
        stuff = query_db_final(query)
    except:
        print('getwantedtrees failed to get the tree info', query)
        return 'FAILURE'
    return str(stuff)

####
# Add another substring representing a tree we want to retrieve
#  Format is IceCube/YEAR/internal-system/pDAQ-2ndBld, where
# YEAR is to be replaced by the year(s) found on the disk
@app.route("/dumping/wantedtrees/<estring>", methods=["POST"])
def addwantedtrees(estring):
    query = 'INSERT INTO WantedTrees (dataTree) VALUES (?)'
    backagain = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    params = (str(backagain), )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('addwantedtree failed to add a wanted data tree', query, params)
        return 'FAILURE'
    return ''

####
# Get expected file count for the directory specified by the
#  tree fragment proffered
@app.route("/dumping/expectedir/<estring>", methods=["GET"])
def getcountexpected(estring):
    directoryfragment = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    query = 'SELECT number from expected WHERE directory LIKE ?'
    params = ('%' + str(directoryfragment) + '%', )
    cc = -1
    try:
        stuff = query_db_final(query, params)
        cc = int(stuff[0]['number'])
    except:
        print('getcountexpected failed to get info for', directoryfragment, stuff)
    return str(cc)

######################
####
# Get the list of directories that we think are complete, for handing
#  off to LTA
@app.route("/dumping/readydir", methods=["GET"])
def givereaddirs():
    query = 'SELECT idealName,toLTA FROM FullDirectories WHERE toLTA=0'
    try:
        stuff = query_db_final(query)
    except:
        print('givereaddirs:  cannot read query', query)
        return 'FAILURE'
    return str(stuff)

####
# Get the list of directories that we consider handed off already
@app.route("/dumping/handedoffdir/<estring>", methods=["GET"])
def givedonedirs(estring):
    ''' If 1 argument, this is anything >0.  If 2, select only the toLTA=2nd '''
    comm = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    wcomm = comm.split()
    if len(wcomm) == 2:
        directoryfragment = wcomm[0]
        query = 'SELECT idealName,toLTA FROM FullDirectories WHERE toLTA=? AND idealName LIKE ?'
        params = (wcomm[1], '%' + str(directoryfragment) + '%')
    else:
        directoryfragment = comm
        query = 'SELECT idealName,toLTA FROM FullDirectories WHERE toLTA>0 AND idealName LIKE ?'
        params = ('%' + str(directoryfragment) + '%', )
    try:
        stuff = query_db_final(query, params)
    except:
        print('givereaddirs:  cannot read query', query, params)
        return 'FAILURE'
    return str(stuff)

####
# Insert a new ready directory
@app.route("/dumping/enteredreadydir/<estring>", methods=["POST"])
def enterreadydir(estring):
    query = 'INSERT INTO FullDirectories (idealName,toLTA) VALUES (?,0)'
    params = (str(unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))), )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('enterreadydirs:  cannot insert directory', query, params)
        return 'FAILURE'
    return ''

####
# Count the ready directories
@app.route("/dumping/countready", methods=["GET"])
def countready():
    # total, unstaged, staged, done
    query = 'SELECT count(*) AS total, sum(case when toLTA=0 then 1 else 0 end) AS unstaged, '
    query = query + 'sum(case when toLTA=1 then 1 else 0 end) AS staged, '
    query = query + 'sum(case when toLTA=2 then 1 else 0 end) AS done, '
    query = query + 'sum(case when toLTA=3 then 1 else 0 end) AS cleaned '
    query = query + 'FROM FullDirectories GROUP BY toLTA;'
    try:
        stuff = query_db_final(query)
    except:
        print('countready:  cannot read query', query)
        return 'FAILURE'
    return str(stuff)

#########################
####
# Get/reset the GlueStatus
@app.route("/glue/status/<estring>", methods=["GET", "POST"])
def getgluestatus(estring):
    # return failure if needed
    # Run means currently running.  Ready means ready to run
    comm = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    if comm not in ['Pause', 'Run', 'Ready', 'Query']:
        return 'Error'
    #
    try:
        stuff = query_db_final('SELECT status from GlueStatus LIMIT 1')
    except:
        print('getgluestatus:  cannot read status from table')
        return 'FAILURE'
    try:
        answer = eval(str(stuff[0]))['status']
    except:
        print('getgluestatus: stuff', str(stuff), str(stuff[0]))
        return 'FAILURE'
    if request.method == "GET" or (request.method == "POST" and comm == "Query"):
        return answer
    #
    if comm == answer:
        return ''
    try:
        stuff = insert_db_final('UPDATE GlueStatus SET status=?,lastChangeTime=datetime(\'now\',\'localtime\')', (comm,))
    except:
        print('getgluestatus set failed', 'UPDATE GlueStatus SET status=?', comm, stuff)
        return 'FAILURE'
    return ''


####
# Set the FullDirectories toLTA as specified
@app.route("/glue/workupdate/<estring>", methods=["POST"])
def glueworkupdate(estring):
    ''' Update directory as Picked or other specified '''
    # don't check for failure
    comm = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    word_pair = comm.split()
    if len(word_pair) == 2:
        query = 'UPDATE FullDirectories SET toLTA=? WHERE idealName LIKE ?'
        params = (word_pair[1], '%' + word_pair[0] + '%')
    else:
        params = (1, '%' + comm + '%')
    try:
        stuff = insert_db_final(query, params)
    except:
        print('glueworkupdate: failed to set FullDirectories toLTA where idealName LIKE', params)
        return 'FAILURE'
    #
    return ''



####
# Set the time for the given type
@app.route("/glue/timeset/<estring>", methods=["POST"])
def gluetimeset(estring):
    ''' Set the time for the given category '''
    comm = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    if comm not in ['LastDumpEnded', 'LastGluePass']:
        return 'FAILURE: bad arg'
    query = 'UPDATE GlueDump SET lastChangeTime=datetime(\'now\',\'localtime\') WHERE type=?'
    params = (comm, )
    try:
        stuff = insert_db_final(query, params)
    except:
        print('gluetimeset: failed to set time for', query, params)
        return 'FAILURE'
    return ''

####
# Return time difference between last Dump and last Glue pass
@app.route("/glue/timediff", methods=["GET"])
def gluetimediff():
    ''' Return the time difference between the last Dump and last Glue pass '''
    query = 'SELECT julianday(lastChangeTime) FROM GlueDump ORDER BY TYPE'
    try:
        stuff = query_db_final(query)
    except:
        print('gluetimediff: failed to get the times', query, stuff)
        return '1000000'
    #
    try:
        vd = float(stuff[0]['julianday(lastChangeTime)'])
        vg = float(stuff[1]['julianday(lastChangeTime)'])
        return str(vd - vg)
    except:
        print('gluetimediff: failed to unpack times', stuff)
        return '2000000'

####
# Get or release token to operate
@app.route("/glue/token/<estring>", methods=["POST"])
def gluetoken(estring):
    ''' Set the token if not taken.  Release if host=RELEASE 
        return host and date if QUERY
        0 if ok, 1 if already taken, 2 if error '''
    host = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    if host == 'RELEASE':
        query = 'DELETE FROM ReviewDump'
        try:
            stuff = insert_db_final(query)
        except:
            print('gluetoken failed to release', stuff)
            return '2'
        return '0'
    query = 'SELECT * FROM ReviewDump'
    try:
        stuff = query_db_final(query)
    except:
        print('gluetoken failed to query', query)
        return '2'
    if host == 'QUERY':
        return str(stuff)
    if len(stuff) > 2:
        return '1'
    query = 'INSERT INTO ReviewDump (host,lastChangeTime) VALUES (?,datetime(\'now\',\'localtime\'))'
    param = (host, )
    try:
        stuff = insert_db_final(query, host)
    except:
        print('gluetoken failed to insert', stuff, query, param)
        return '2'
    return '0'

####
# Get or release or query token to operate Deleter
@app.route("/glue/deleter/<estring>", methods=["POST"])
def gluedeletetoken(estring):
    ''' Set the token if not taken.  Release if host=RELEASE
        return host and date if QUERY
        return 0 if ok, 1 if not available, 2 if error '''
    host = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    if host == 'RELEASE':
        query = 'DELETE FROM DeleterToken'
        try:
            stuff = insert_db_final(query)
            return '0'
        except:
            print('gluedeletetoken failed to release', stuff)
            return '2'
    #
    query = 'SELECT * FROM DeleterToken'
    try:
        stuff = query_db_final(query)
    except:
        print('gluedeletetoken failed to query', query)
        return '2'
    if host == 'QUERY':
        return str(stuff)
    if len(str(stuff)) > 2:
        return '1'
    query = 'INSERT INTO DeleterToken (hostname,lastChangeTime) VALUES (?,datetime(\'now\',\'localtime\'))'
    param = (host, )
    try:
        stuff = insert_db_final(query, param)
    except:
        print('gluedeletetoken failed to insert', stuff, query, param)
        return '2'
    return '0'


####
# Add a new directory to the full list 
@app.route("/directory/<estring>", methods=["POST"])
def adddirectory(estring):
    ''' Add the directory to the list with the default status '''
    filename = unmangle(urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"'))
    param = (filename, )
    query = 'INSERT INTO FullDirectory (idealName,status,lastChangeTime) VALUES (?,\'unclaimed\',datetime(\'now\',\'localtime\'))'
    try:
        stuff = insert_db_final(query, param)
        return ''
    except:
        print('adddirectory failed to insert', filename, stuff)
        return 'FAILURE to insert'

####
# Query the table.  Must expand greatly, this is a rump
@app.route("/directory/info/<estring>", methods=["GET"])
def querydirectory(estring):
    ''' Return information about the rows
        0th order:  use an int and only get the dirkey
        final version--send a dict with complex query '''
    try:
        dirkey = int(estring)
    except:
        untangle = unmangle(reslash(estring).replace('\'', '\"'))
        try:
            trial_json = json.loads(untangle)
        except:
            print('querydirectory has non-int and non-json.  Something is wrong', untangle)
            return 'FAILURE argument non-json'
        # Extract possible options
        try:
            dirkey = trial_json['dirkey']
        except:
            dirkey = None
        try:
            likeIdeal = trial_json['likeIdeal']
        except:
            likeIdeal = None
        try:
            status = trial_json['status']
        except:
            status = None
        # Not all of these are needed.  If dirkey is present, it takes precedence
    if dirkey is not None:
        param = (dirkey, )
        query = 'SELECT * FROM FullDirectory where dirkey=?'
        try:
            stuff = query_db_final(query, param)
            return str(stuff)
        except:
            print('querydirectory failed the query', query, param, stuff)
            return 'FAILURE to query'
    #
    if likeIdeal is None and status is None:
        # Something got screwed up
        print('querydirectory got no useful test parameters', untangle)
        return 'FAILURE no useful parameters'
    if likeIdeal is not None and status is None:
        param = ('%' + likeIdeal + '%', )
        query = 'SELECT * FROM FullDirectory where idealName LIKE ?'
    if likeIdeal is not None and status is not None:
        param = ('%' + likeIdeal + '%', status)
        query = 'SELECT * FROM FullDirectory where idealName LIKE ? AND status=?'
    if likeIdeal is None and status is not None:
        param = (status, )
        query = 'SELECT * FROM FullDirectory where status=?'
    try:
        stuff = query_db_final(query, param)
        return str(stuff)
    except:
        print('querydirectory failed the query', query, param, stuff)
        return 'FAILURE to query'

####
# modify a directory entry
@app.route("/directory/modify/<estring>", methods=["POST"])
def modifydirectory(estring):
    ''' Modify a directory entry with a new status, and possibly other info '''
    # FULL_DIR_STATI
    untangle = unmangle(reslash(estring).replace('\'', '\"'))
    words = untangle.split()
    if len(words) <= 1 or words[0] not in FULL_DIR_STATI:
        print('modifydirectory has no idea what to do with', untangle, ' expect key status')
        return 'FAILURE bad arguments'
    # Simple updates first
    if len(words) == 2:
        if words[1] in FULL_DIR_SINGLE_STATI:
            param = (words[1], words[0])
            query = 'UPDATE FullDirectory SET status=?,lastChangeTime=datetime(\'now\',\'localtime\') WHERE dirkey=?'
        else:
            print('modifydirectory has inadequate argument count for the command', untangle, ' expect key status')
            return 'FAILURE arguments bad'
    else:
        if words[1] == 'processing':
            # Expect dirkey, newstatus, hostname, process_id
            if len(words) != 4:
                 print('modifydirectory has too few arguments for processing', untangle, ' expect key status host processid')
                 return 'FAILURE arguments bad 4'
            param = (words[1], words[2], words[3], words[0])
            query = 'UPDATE FullDirectory SET status=?,hostname=?,process=?,lastChangeTime=datetime(\'now\',\'localtime\') where dirkey=?'
        if words[1] == 'LTArequest':
            if len(words) != 3:
                print('modifydirectory has too few arguments for LTArequest', untangle, 'expect key status requestid')
                return 'FAILURE arguments bad 3'
            param = (words[1], words[2], words[0])
            query = 'UPDATE FullDirectory SET status=?,requestid=?,lastChangeTime=datetime(\'now\',\'localtime\') WHERE dirkey=?'
    try:
        stuff = query_db_final(query, param)
        return str(stuff)
    except:
        print('modifydirectory failed the update', query, param, stuff)
        return 'FAILURE to update'
    

###################################################
#####
# OK, now the main code
#db = SQLAlchemy(app)
#migrate = Migrate(app, db)


if __name__ == "__main__":
    #app.run(debug=True, host='0.0.0.0')
    app.run(host='0.0.0.0', port=80)
