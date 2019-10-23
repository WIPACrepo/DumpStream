DBdatabase = None
DBcursor = None
######################################################
######
# DB connection established
def getdbgen():
    global DBdatabase
    global DBcursor
    try:
        # https://stackoverflow.com/questions/27203902/cant-connect-to-database-pymysql-using-a-my-cnf-file
        DBdatabase = pymysql.connect(read_default_file='~/.my.cnf',)
    except pymysql.OperationalError:
        print(['ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(1)
    except Exception:
        print(['ERROR: Generic failure to connect to MySQL archivedump database.', sys.exc_info()[0]])
        sys.exit(1)
    try:
        #DBcursor = DBdatabase.cursor()
        DBcursor = DBdatabase.cursor(pymysql.cursors.DictCursor)
    except pymysql.OperationalError:
        print(['ERROR: could not get cursor for database.'])
        sys.exit(1)
    except Exception:
        print(['ERROR: Generic failure to get cursor for database.', sys.exc_info()[0]])
        sys.exit(1)
    return

####
def returndbgen():
    return DBcursor

####
def closedbgen():
    DBcursor.close()
    DBdatabase.close()
    return

####
# Commit changes to DB specified
def doCommitDB():
    try:
        DBdatabase.commit()
    except pymysql.OperationalError:
        DBdatabase.rollback()
        print(['doCommitDB: ERROR: could not connect to MySQL archivedump database.'])
        sys.exit(1)
    except Exception:
        DBdatabase.rollback()
        print(['doCommitDB: Failed to execute the commit'])
        sys.exit(1)



############################################
######  Execute a command.  Crash if it fails, otherwise return silently
def doOperationDB(dbcursor, command, string):
    try:
        dbcursor.execute(command)
        return
    except pymysql.OperationalError:
        print(['ERROR: doOperationDB could not connect to MySQL ', string, ' database.', command])
        sys.exit(1)
    except Exception:
        print(['ERROR: doOperationDB undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(1)
    return

############################################
######  Execute a command.  Crash if it fails, return False if it didn't work right, True if it was OK
def doOperationDBWarn(dbcursor, command, string):
    try:
        dbcursor.execute(command)
        return True
    except pymysql.OperationalError:
        print(['ERROR: doOperationDBWarn could not connect to MySQL ', string, ' database.', command])
        sys.exit(1)
    except pymysql.IntegrityError:
        print(['ERROR: doOperationDBWarn \"IntegrityError\", probably duplicate key', string, ' database.', sys.exc_info()[0], command])
        return False
    except Exception:
        print(['ERROR: doOperationDBWarn undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command])
        sys.exit(1)
    return True

############################################
######  Execute a command, return the answer.  Or error messages if it failed
def doOperationDBTuple(dbcursor, command, string):
    try:
        dbcursor.execute(command)
        expectedtuple = dbcursor.fetchall()
        return expectedtuple		#Assume you know what you want to do with this
    except pymysql.OperationalError:
        return ['ERROR: doOperationDBTuple could not connect to MySQL ', string, ' database.', command]
    except Exception:
        return ['ERROR: doOperationDBTuple undefined failure to connect to MySQL ', string, ' database.', sys.exc_info()[0], command]
    return [[]]
