# Utils.py (.base)
''' Define a lot of constants and utility routines for my REST framework.
    Some wheel-reinventing here:  I would do things differently now.
    MySQL stuff. '''
import sys
import pymysql

#
# MySQL database pointers
DBdatabase = None
DBcursor = None

#
######################################################
######
# DB connection established
def getdbgen():
    ''' Open connection to mysql database '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	print message if problem
    #			crash if problem
    #			possible change in database
    # Relies on:	mysql database working
    #			~/.my.cnf has valid credentials
    #			global DBdatabase
    #			global DBcursor
    #-
    global DBdatabase
    global DBcursor
    try:
        # https://stackoverflow.com/questions/27203902/cant-connect-to-database-pymysql-using-a-my-cnf-file
        DBdatabase = pymysql.connect(read_default_file='~/.my.cnf',)
    except pymysql.OperationalError:
        print(['ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(11)
    except Exception:
        print(['ERROR: Generic failure to connect to MySQL archivedump database.', sys.exc_info()[0]])
        sys.exit(11)
    try:
        #DBcursor = DBdatabase.cursor()
        DBcursor = DBdatabase.cursor(pymysql.cursors.DictCursor)
    except pymysql.OperationalError:
        print(['ERROR: could not get cursor for database.'])
        sys.exit(11)
    except Exception:
        print(['ERROR: Generic failure to get cursor for database.', sys.exc_info()[0]])
        sys.exit(11)

####
def returndbgen():
    ''' Return cursor to mysql database '''
    #+
    # Arguments:	None
    # Returns:		cursor to database.  Hope it is active!
    # Side Effects:	None
    # Relies on:	global DBcursor
    #-
    global DBcursor
    #
    return DBcursor

####
def closedbgen():
    ''' Disconnect cursor and database connection to mysql database '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	Disconnects from database
    # Relies on:	global DBcursor
    #			global DBdatabase
    #-
    global DBdatabase
    global DBcursor
    #
    DBcursor.close()
    DBdatabase.close()

####
# Commit changes to DB specified
def doCommitDB():
    ''' Commit changes to mysql database.  DumpStream code does not use this '''
    #+
    # Arguments:	None
    # Returns:		None
    # Side Effects:	print message if problem
    #			crash if problem
    #			possible change in database
    # Relies on:	mysql database working
    #			global DBdatabase
    #-
    global DBdatabase
    #
    try:
        DBdatabase.commit()
    except pymysql.OperationalError:
        DBdatabase.rollback()
        print(['doCommitDB: ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(11)
    except Exception:
        DBdatabase.rollback()
        print(['doCommitDB: Failed to execute the commit'])
        sys.exit(11)



############################################
######  Execute a command.  Crash if it fails, otherwise return silently
def doOperationDB(dbcursor, command, string):
    ''' Execute a mysql command, crash if failure, return nothing '''
    #+
    # Arguments:	active mysql db cursor
    #			mysql command
    #			string to print out if failure
    # Returns:		None
    # Side Effects:	print message if problem
    #			crash if problem
    #			possible change in database
    # Relies on:	mysql database working
    #-
    try:
        dbcursor.execute(command)
        return
    except pymysql.OperationalError:
        print(['ERROR: doOperationDB could not connect to MySQL ', string, ' database.', command])
        sys.exit(11)
    except Exception:
        print(['ERROR: doOperationDB undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(11)
    return

############################################
######  Execute a command.  Crash if it fails, return False if it didn't work right, True if it was OK
def doOperationDBWarn(dbcursor, command, string):
    ''' Execute a mysql command, return success '''
    #+
    # Arguments:	active mysql db cursor
    #			mysql command
    #			string to print out if failure
    # Returns:		boolean for success or failure
    # Side Effects:	print message if problem
    #			crash if connection failure
    #			possible change in database
    # Relies on:	mysql database working
    #-
    try:
        dbcursor.execute(command)
        return True
    except pymysql.OperationalError:
        print(['ERROR: doOperationDBWarn could not connect to MySQL ', string, ' database.', command])
        sys.exit(11)
    except pymysql.IntegrityError:
        print(['ERROR: doOperationDBWarn \"IntegrityError\", probably duplicate key', string, ' database.', sys.exc_info()[0], command])
        return False
    except Exception:
        print(['ERROR: doOperationDBWarn undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(11)
    return True


############################################
######  Execute a command, return the answer.  Or error messages if it failed
def doOperationDBTuple(dbcursor, command, string):
    ''' Execute a database operation on a mysql database w/ dbcursor established
        The command is command
        string is info '''
    #+
    # Arguments:	dbcursor:  cursor established for mysql database
    #			command:  mysql command to execute
    #			string:	info to print in case of error
    # Returns:		tuple, contents depend on the command
    #			list with error info
    # Side Effects:	mysql DB may change; depends on command
    # Relies on:	mysql DB available, connection works
    #			pymysql
    #-
    try:
        dbcursor.execute(command)
        expectedtuple = dbcursor.fetchall()
        return expectedtuple		#Assume you know what you want to do with this
    except pymysql.OperationalError:
        return ['ERROR: doOperationDBTuple could not connect to MySQL ', string, ' database.', command]
    except Exception:
        return ['ERROR: doOperationDBTuple undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command]
    return [[]]
