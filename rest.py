import sys
import site
python_home = '/opt/testing/rest/venv'
#Calc the path to site_packages directory
python_version = '.'.join(map(str, sys.version_info[:2]))
site_packages = python_home + '/lib/python%s/site-packages' % python_version

## Add the site-packages directory.
#site.addsitedir(site_packages)

# Remember original sys.path.
if site_packages not in list(sys.path):
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
DUMPSTATI = ['Run', 'Halt', 'Drain', 'Error']
NERSCSTATI = ['Run', 'Halt', 'DrainNERSC', 'Error']
BUNDLESTATUSCOLUMNS = []
DEBUGDB = False


# String manipulation stuff
def unslash(strWithSlashes):
    return strWithSlashes.replace('/', REPLACESTRING)

def reslash(strWithoutSlashes):
    return strWithoutSlashes.replace(REPLACESTRING, '/')

def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace(r'\,', ',').replace('@', ' ')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', r'\,').replace(' ', '@')

def tojsonquotes(strFromPost):
    # Turn single into double quotes
    return strFromPost.replace("\'", "\"")

def fromjsonquotes(strFromPost):
    # Turn double into single quotes.  Won't use it much
    # here, but the remote jobs that feed this will
    return strFromPost.replace("\"", "\'")

def kludgequote(strFromPost):
    return strFromPost.replace("\\\"", "\"")

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
        print("Do close_connection")
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
        print("Do query_db")
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
        print("Do query_db_final")
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
    newstr = newstr + ' VALUES (\'' + stuff[0]['bundleError'] + '\',' + str(stuff[0]['bundlePoolSize']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + estring + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

####
# Insert a row updating the local pool size (should be quite large).
#  Also serves as heartbean for the local scanning system.
@app.route("/dumpcontrol/update/poolsize/<estring>", methods=["POST"])
def updatedumppoolsize(estring):
    try:
        newint = int(estring)
    except:
        return 'Not an integer'
    getallstr = 'SELECT bundleError,status FROM DumpCandC ORDER BY dumpCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + stuff[0]['bundleError'] + '\',' + estring
    newstr = newstr + ',datetime(\'now\',\'localtime\'),\'' + stuff[0]['status'] + '\')'
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
        berror = stuff[0]['status']   # If clearing it, leave the status the same
    newstr = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + revisedstring + '\',' + str(stuff[0]['bundlePoolSize']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + berror + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

####
# Insert a row to reset the dump control to Run, clearing errors
@app.route("/dumpcontrol/update/reset", methods=["POST"])
def updatedumpreset():
    #
    stuff = query_db('SELECT bundlePoolSize from DumpCandC order by dumpCandC_id DESC LIMIT 1')
    bps = stuff[0]['bundlePoolSize']
    updatestring = 'INSERT INTO DumpCandC (bundleError,bundlePoolSize,lastChangeTime,status) \
        VALUES ("",' + bps + ',datetime(\'now\',\'localtime\'),"Run")'
    stuff = insert_db_final(updatestring)
    # Put in sanity checking
    return 'OK'

# This is a debugging entry.  Delete at leisure
@app.route("/dumpcontrol/<listostuff>", methods=["GET", "POST"])
def dumpcontrol(listostuff):
    print('dumpcontrol/', listostuff + '===')
    for i in request.args:
        print(i)
    newlistostuff = reslash(listostuff)
    if len(newlistostuff) > 0:
        print(newlistostuff)
    else:
        print("listostuff was empty")
    curdb = get_db()
    try:
        poolsize = int(newlistostuff)
        stuff = query_db('SELECT * from DumpCandC WHERE bundlePoolSize> ' + newlistostuff + ' order by dumpCandC_id DESC limit 1')
    except:
        stuff = query_db('SELECT status,bundlePoolSize from DumpCandC order by dumpCandC_id DESC limit 1')
    if len(stuff) <= 0:
        return ''
    return stuff[0]


##############

## nersccontrol

# Info methods; 3 URLs to do the same thing
@app.route("/nersccontrol")
@app.route("/nersccontrol/info", methods=["GET", "POST"])
@app.route("/nersccontrol/info/", methods=["GET", "POST"])
def nersccontrolinfo():
    stuff = query_db('SELECT * FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1')
    return stuff[0]

# Update methods

# Insert a row to change the status of the NERSC loading control
@app.route("/nersccontrol/update/status/<estring>", methods=["POST"])
def updatenerscstatus(estring):
    #
    if estring not in NERSCSTATI:
        return 'Must be one of ' + str(NERSCSTATI)
    getallstr = 'SELECT nerscSize,localError FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO NERSCandC (localError,nerscSize,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + stuff[0]['localError'] + '\',' + str(stuff[0]['nerscSize']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + estring + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

# Insert a row to update the NERSC pool size.  This also serves as a heartbeat
# for the NERSC loading control
@app.route("/nersccontrol/update/poolsize/<estring>", methods=["POST"])
def updatenerscpoolsize(estring):
    #
    print('update/poolsize/', estring)
    securityCheck()
    try:
        newint = int(estring)
    except:
        return 'Not an integer'
    getallstr = 'SELECT localError,nerscError,status FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    newstr = 'INSERT INTO NERSCandC (localError,nerscError,nerscSize,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + stuff[0]['localError'] + '\',\'' + stuff[0]['nerscError'] + '\','
    newstr = newstr + estring
    newstr = newstr + ',datetime(\'now\',\'localtime\'),\'' + stuff[0]['status'] + '\')'
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

# Insert a row to update the NERSC loader control error message.  Changes the
# status if appropriate
@app.route("/nersccontrol/update/nerscerror/<estring>", methods=["POST"])
def updatenerscerror(estring):
    #
    revisedstring = unmangle(estring)
    getallstr = 'SELECT nerscSize,localError,status FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    if len(revisedstring) > 0:
        # Error--change status automatically
        berror = 'Error'
    else:
        berror = stuff[0]['status']   # If clearing it, leave the status the same
    newstr = 'INSERT INTO NERSCandC (nerscError,localError,nerscSize,lastChangeTime,status)'
    newstr = newstr + ' VALUES (\'' + revisedstring + '\',\'' + stuff[0]['localError'] + '\','
    newstr = newstr + str(stuff[0]['nerscSize']) + ','
    newstr = newstr + 'datetime(\'now\',\'localtime\'),\'' + berror + '\')'
    print(newstr)
    stuff = insert_db_final(newstr)
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
    getallstr = 'SELECT nerscSize,nerscError,status FROM NERSCandC ORDER BY nerscCandC_id DESC LIMIT 1'
    stuff = query_db(getallstr)
    if len(revisedstring) > 0:
        # Error--change status automatically
        berror = 'Error'
    else:
        berror = stuff[0]['status']   # If clearing it, leave the status the same
    newstr = 'INSERT INTO NERSCandC (localError,nerscError,nerscSize,lastChangeTime,status)\
        VALUES (\'{}\',\'{}\',{},datetime(\'now\',\'localtime\'),\'{}\')'.format(revisedstring, stuff[0]['nerscError'], str(stuff[0]['nerscSize']), berror)
    stuff = insert_db_final(newstr)
    # Put in sanity checking
    return 'OK'

# Insert a row setting the NERSC control to Run, clearing the error
@app.route("/nersccontrol/update/reset", methods=["POST"])
def updatenerscreset():
    #
    stuff = query_db('SELECT nerscSize from NERSCandC order by nerscCandC_id DESC LIMIT 1')
    bps = stuff[0]['nerscSize']
    updatestring = 'INSERT INTO NERSCandC (localError,nerscError,nerscSize,lastChangeTime,status) \
        VALUES ("","",{},datetime(\'now\',\'localtime\'),"Run")'.format(bps)
    stuff = insert_db_final(updatestring)
    # Put in sanity checking
    return 'OK'



### Heartbeat methods

@app.route("/nerscheartbeat/<estring>", methods=["GET", "POST"])
def nerscheartbeat(estring):
    if request.method == 'GET':
        sstring = 'SELECT * FROM Heartbeats where hostname=\"{}\"'.format(estring)
        stuff = query_db_final(sstring)
        #stuff = query_db('SELECT * FROM Heartbeats where hostname=\"' + estring + '\"')
        if len(stuff) == 0:
            return "404 Not Found"
        return stuff[0]    # Let the remote user figure it out.  Keep it simple
    sstring = 'UPDATE Heartbeats set lastChangeTime=datetime(\'now\',\'localtime\') WHERE hostname=\"{}\"'.format(estring)
    stuff = insert_db_final(sstring)
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
        return "BUSY"
    else:
        # 2 Cases:  Case 1, the string is blank.  Set it (take token), return OK
        #  Case 2, the string is not blank.  Return BUSY
        if str(answer[0]) == '':
            string = 'UPDATE Token SET hostname=\"{}\",lastChangeTime=datetime(\'now\',\'localtime\')'.format(estring)
            stuff = insert_db_final(string)
            if len(stuff) > 1:
                # What went wrong?
                print(str(stuff))
                return "BUSY"
            return "OK"
        else:
            # Somebody else (maybe another process on the same system?)
            # has the token
            return "BUSY"

# Release the token
@app.route("/nersctokenrelease", methods=["POST"])
@app.route("/nersctokenrelease/", methods=["POST"])
def nerscrelease():
    answer = query_db('UPDATE Token SET hostname=\"\",lastChangeTime=datetime(\'now\',\'localtime\')')
    if len(answer) > 1:
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
    print(answer)
    if len(answer) < 1:
        return ""
    return str(answer)

#####
#

### Bundle methods

# Get the oldest untouched bundle.  I.E. -- the next one to start processing
@app.route("/bundles/oldestuntouched", methods=["GET"])
@app.route("/bundles/oldestuntouched/", methods=["GET"])
def getoldestuntouched():
    qstring = 'SELECT * FROM BundleStatus WHERE status=\"Untouched\" ORDER BY bundleStatus_id ASC LIMIT 1'
    stuff = query_db_final(qstring)
    # sanity checking?
    return stuff[0]

# Get the bundle satisfying certain criteria.
# The default is the oldest untouched, but you can specify anything,
# and may get all of them if that's your pleasure
@app.route("/bundles/specified/<estring>", methods=["GET"])
def getspecified(estring):
    if not estring:
        qstring = 'SELECT * FROM BundleStatus WHERE status=\"Untouched\" ORDER BY bundleStatus_id ASC LIMIT 1'
        stuff = query_db_final(qstring)
        # sanity checking?
        return stuff[0]
    unstring = kludgequote(unmangle(estring))
    print(unstring)
    qstring = 'SELECT * FROM BundleStatus WHERE {}'.format(unstring)
    try:
        print(qstring)
        stuff = query_db_final(qstring)
        if len(stuff) > 0:
            return str(stuff)
        else:
            return ""
    except:
        print(str(stuff))
        return ""

# Update the specified bundle.  Wants the bundleStatus_id
@app.route("/updatebundle/<estring>", methods=["GET", "POST"])
def updatebundle(estring):
    # Yes, of course.  I use the string as the command.  All sorts
    # of horrors are possible
    unstring = kludgequote(unmangle(estring))
    try:
        print(unstring)
        stuff = query_db_final(unstring)
        if len(stuff) > 0:
            return str(stuff[0])
        else:
            return ""
    except:
        print("Failed with", unstring)
        return ""

### Insertion methods for bundles.  These aren't updates, but new bundles
# This needs some debugging yet.  IT DOES NOT HAVE THE DB INSERTION CODE YET
@app.route("/addbundle/<estring>", methods=["POST"])
def addbundle(estring):
    backagain = urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"')
    try:
        fjson = (json.loads(backagain))
    except:
        print(backagain)
        return "Not valid json"
    #try:
    #    for x in fjson:
    #        print("{} had {}".format(x, fjson[x]))
    #except:
    #    return "Problem with extraction of json"

    try:
        lname = fjson['localName']
    except:
        return "json does not include localName"
    # Init if not done already--this is a global
    if len(BUNDLESTATUSCOLUMNS) == 0:
        schem = open('./BundleStatus.schema', 'r')
        for line in schem:
            words = line.split()
            if len(words) <= 1:
                continue
            if words[0] == 'CREATE' and words[1] == 'TABLE':
                continue
            BUNDLESTATUSCOLUMNS.append(words[0])
        schem.close()
    # 
    qstring = 'SELECT * FROM BundleStatus WHERE localName=\"{}\"'.format(lname)
    try:
        stuff = query_db(qstring)
        print(stuff)
    except:
        print("Some kind of error")
        return "Not OK"
    if len(stuff) > 1:
        if stuff[0]['bundleStatus_id']:
            print("No already existing")
            return "already exists, insert forbidden"
    initialstring = "INSERT INTO BundleStatus (localName,idealName,size,checksum,UUIDJade,UUIDGlobus,useCount,status) VALUES"
    # Sanity check
    for inargs in fjson:
        if inargs not in BUNDLESTATUSCOLUMNS:
            return inargs + " is not a valid database column for BundleStatus"
    initialstring = initialstring + "(\"" + fjson['localName'] + "\",\""
    idealName = '/data/exp' + str(fjson['localName']).split('data/exp')[1]
    initialstring = initialstring + idealName + "\"," + str(fjson['size']) + ",\""
    initialstring = initialstring + str(fjson['checksum']) + "\",\"\",\"\",1,\"Untouched\")"
    print(initialstring)
    return "OK done"


### Debugging methods.  Here I'm doing the testing of json unpacking for bundling
# IT DOES NOT HAVE DB INSERTION CODE
@app.route("/debugging/<estring>", methods=["GET", "POST"])
def fiddling(estring):
    #print(estring)
    #print(reslash(estring))
    backagain = urllib.parse.unquote_plus(reslash(estring)).replace('\'', '\"')
    #print(backagain)
    try:
        fjson = (json.loads(backagain))
    except:
        print(backagain)
        return "Problem with json.loads"
    try:
        for x in fjson:
            print("{} had {}".format(x, fjson[x]))
        #return "OK"
    except:
        return "Problem with extraction of json"
    lname = fjson['localName']
    qstring = 'SELECT bundleStatus_id FROM BundleStatus WHERE localName=\"{}\"'.format(lname)
    print(qstring)
    try:
        stuff = query_db(qstring)
        print(stuff)
    except:
        print("Some kind of error")
        return "Not OK"
    #
    qstring = 'SELECT * FROM BundleStatus WHERE localName=\"{}\"'.format(lname)
    stuff = query_db(qstring)
    for indb in stuff[0]:
        if stuff[0][indb] != fjson[indb]:
            print(indb, stuff[0][indb], fjson[indb])
    return "OK done"

#####
# OK, now the main code
#db = SQLAlchemy(app)
#migrate = Migrate(app, db)


if __name__ == "__main__":
    #app.run(debug=True, host='0.0.0.0')
    app.run(host='0.0.0.0', port=80)
