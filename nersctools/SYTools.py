""" SYTools.py contains tools for logging and spawning

myprint             Standard logging tool
myprintlog          Fallback logging
initSystemFromXML   Initialize the system:  robot and target directories
RobotSanity         Check that this computer can see the robot devices
DirectorySanity     Check that this computer can see the directories
dospawn             Generic spawn
dosimplecommand     Generic simple command (e.g. mv)
dosimplecommandtimeout   Generic simple command with timeout in seconds
getoutputsimplecommand   Generic simple command returning output (e.g. ls)
parserun            Get the run/subrun/part numbers from the file name
checkexistence      Given run/sub/part and lists, has this been done yet?
tarupRaw            Tar up the dump and checksum logs
MakeSubmit          Create a submit file to run the checksums
"""
# ~sdstarchive/ARCHIVE/dumptape/SYTools.py
# Holds the tools for printing, spawning, etc.
#
import sys
import datetime
#import socket
#from getopt import getopt
import os
import subprocess
#from subprocess import Popen, PIPE
import xml.etree.ElementTree as ET
#import glob
import string
#from multiprocessing import Process, Queue

######################################################

# inits
DRIVECOUNT = 0
DRIVEMAP = []
CONTROLLERMAP = ""
SLOTCOUNT = 0
TOPRAWDIR = ""
TOPSDSTDIR = ""

LOGFILE = ""
TAPEDIR = ""
XMLFILE = "/home/sdstarchive/ARCHIVE/dumptape/julyconfig.xml"

ENTRY_ID = ""
START_DATETIME = ""
END_DATETIME = ""
FIRST_EVENT = 0
LAST_EVENT = 0

######
# Definitions


def myprint(stuff):
    """myprint(stuff) writes the line of 'stuff' out to the file opened as

    LOGFILE, prefixed with the date and time.  There is a default for
    LOGFILE, though this can be changed easily
    """
    if len(stuff) > 0:
        try:
            lfile = open(LOGFILE, 'a')
        except Exception:
            myprintlog(["Could not open -", LOGFILE, "- as output, exiting."])
            sys.exit(1)
        htime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            lfile.write(htime)
            lfile.write(" " + sys.argv[0] + ": ")
            for word in stuff:
                lfile.write(str(word) + " ")
            lfile.write("\n")
        except Exception:
            myprintlog([sys.exc_info()[0]])
            myprintlog(stuff)
            myprintlog(["Could not write to -", LOGFILE, "-, exiting."])
            sys.exit(1)
        try:
            lfile.close()
        except Exception:
            myprintlog(["Problem closing -", LOGFILE, "-, exiting."])
            sys.exit(1)

def myprintlog(stuff):
    """myprintlog(stuff) writes the line of 'stuff' out to the standard output

    prefixed with the date and time.  This is a fallback utility.
    """
    if len(stuff) > 0:
        h = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            print(h + " " + sys.argv[0] + ": ", stuff)
        except Exception:
            sys.exit(2)

#######

def initSystemFromXML():
    """initSystemFromXML initializes target directory names and

    tape library drive and controller details from the XML file
    specified by the name XMLFILE.  There is a default.
    """
    global DRIVECOUNT
    global DRIVEMAP
    global SLOTCOUNT
    global TOPRAWDIR
    global TOPSDSTDIR
    global CONTROLLERMAP
    #
    try:
        root = ET.parse(XMLFILE)
    except Exception:
        myprint(["Failed to open or parse -", XMLFILE, "- as the xml configuration file"])
        sys.exit(1)
    #
    DRIVECOUNT = int(root.findtext("./library/drivecount"))
    DRIVEMAP = []
    lis = root.findall("./library/drivemap")
    if len(lis) != DRIVECOUNT:
        myprint(["Inconsistency in", XMLFILE])
        return False
    for i in range(0, DRIVECOUNT):
        DRIVEMAP.append(lis[i].text)
    CONTROLLERMAP = root.findtext("./library/controllermap")
    SLOTCOUNT = int(root.findtext("./library/slotcount"))
    TOPRAWDIR = root.findtext("./space/toprawdir")
    TOPSDSTDIR = root.findtext("./space/topsdstdir")
    return True

######

def RobotSanity():
    """RobotSanity() checks if this computer can see the robot's devices

    If not, either the job wasn't initialized or the computer isn't
    connected to the robot.
    """
    if SLOTCOUNT <= 0 or DRIVECOUNT <= 0:
        return False
    if not os.path.exists(CONTROLLERMAP):
        return False
    for x in DRIVEMAP:
        if    not os.path.exists(x):
            return False
    return True

def DirectorySanity():
    """DirectorySanity() checks if the target directories are visible

    If not, either the job wasn't initialized or something is wrong
    with the filesystem mounts.
    """
    if not os.path.exists(TOPRAWDIR):
        return False
    if not os.path.exists(TOPSDSTDIR):
        return False
    return True
######


def dospawn(cmd):
    """ dospawn(cmd) This spawns a shell process, which will

    write any output back into the stdout of this process.
    The original process can carry on.
    The command is an array of strings.
    """
    try:
        proc = subprocess.Popen(cmd, shell=True)
        return 0
    except subprocess.CalledProcessError:
        myprint([cmd, " Failed to spawn"])
        return -1
    except Exception:
        myprint([cmd, " Unknown error", sys.exc_info()[0]])
        return -1

######

def dosimplecommand(cmd):
 try:
  proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = proc.communicate()
  if error != "":
   myprint([cmd, error])
   return -1
  else:
   return 0
 except subprocess.CalledProcessError:
  #myprintlog([cmd, " Failed to spawn"])
  myprint([cmd, " Failed to spawn"])
  return -2
 except Exception:
  #myprintlog([cmd, " Unknown error", sys.exc_info()[0]])
  myprint([cmd, " Unknown error", sys.exc_info()[0]])
  return -3

######

def dosimplecommandtimeout(cmd, timeout):
 try:
  cmd2 = list(cmd)
  cmd2.insert(0, str(timeout))
  cmd2.insert(0, 'timeout')
  proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = proc.communicate()
  if error != "":
   myprint([cmd, ' with timeout=', str(timeout), ' => ', error])
   return -1
  else:
   return 0
 except subprocess.CalledProcessError:
  myprint([cmd, " Failed to spawn"])
  return -2
 except Exception:
  myprint([cmd, " Unknown error", sys.exc_info()[0]])
  return -3

######

def dosimplecommandtimeoutRobot(cmd, timeout):
    try:
        cmd2 = list(cmd)
        cmd2.insert(0, str(timeout))
        cmd2.insert(0, 'timeout')
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        if error != "":
            if error.find('Request Sense') >= 0:
                return -4
            myprint([cmd, ' with timeout=', str(timeout), ' => ', error])
            return -1
        else:
            return 0
    except subprocess.CalledProcessError:
        myprint([cmd, " Failed to spawn"])
        return -2
    except Exception:
        myprint([cmd, " Unknown error", sys.exc_info()[0]])
        return -3

######

def dosimplecommandtimeoutRobotError(cmd, timeout):
    try:
        cmd2 = list(cmd)
        cmd2.insert(0, str(timeout))
        cmd2.insert(0, 'timeout')
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        if error != "":
            if error.find('Request Sense') >= 0:
                return -4,''
            myprint([cmd, ' with timeout=', str(timeout), ' => ', error])
            return -1,''
        else:
            return 0,''
    except subprocess.CalledProcessError:
        myprint([cmd, " Failed to spawn"])
        return -2,'CalledProcessError'
    except Exception:
        myprint([cmd, " Unknown error", sys.exc_info()[0]])
        return -3,sys.exc_info()[0]
######

def dosimplescript(cmd):
 try:
  proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  output, error = proc.communicate()
  if error != "":
   myprint([cmd, error])
   return -1
  else:
   return 0
 except subprocess.CalledProcessError:
  myprint([cmd, " Failed to spawn"])
  return -2
 except Exception:
  myprint([cmd, " Unknown error", sys.exc_info()[0]])
  if len(sys.exc_info()) > 1:
      myprint([sys.exc_info()[1]])
  return -3

######

def getoutputsimplecommand(cmd):
 try:
  proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = proc.communicate()
  if error != "":
   myprint([cmd, error])
   return ""
  else:
   return output
 except subprocess.CalledProcessError:
  myprint([cmd, " Failed to spawn"])
  return ""
 except Exception:
  myprint([cmd, " Unknown error", sys.exc_info()[0]])
  return ""

def getoutputerrorsimplecommand(cmd):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        returncode = proc.returncode
        return output, error, returncode
    except subprocess.CalledProcessError:
        myprint([cmd, " Failed to spawn"])
        return "", "", 1
    except Exception:
        myprint([cmd, " Unknown error", sys.exc_info()[0]])
        return "", error, 1
######

def getoutputsimplecommandtimeout(cmd, timeout):
 # Note that I want to get the output if there was a timeout!
 try:
  cmd2 = list(cmd)
  cmd2.insert(0, str(timeout))
  cmd2.insert(0, 'timeout')
  output = ""
  proc = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = proc.communicate()
  if error != "":
   myprint([cmd, ' with timeout=', str(timeout), ' => ', error])
   return output
  else:
   return output
 except subprocess.CalledProcessError:
  myprint([cmd, " Failed to spawn"])
  return output
 except Exception:
  myprint([cmd, " Unknown error", sys.exc_info()[0]])
  return output

######

def getoutputerrorsimplecommandtimeout(cmd, timeout):
    # Note that I want to get the output if there was a timeout!
    try:
        cmd2 = list(cmd)
        cmd2.insert(0, str(timeout))
        cmd2.insert(0, 'timeout')
        proc = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = ""
        output, error = proc.communicate()
        if error != "":
            myprint([cmd, ' with timeout=', str(timeout), ' => ', error])
            return output, error
        else:
            return output,error
    except subprocess.CalledProcessError:
        myprint([cmd, " Failed to spawn"])
        return output, error
    except Exception:
        myprint([cmd, " Unknown error", sys.exc_info()[0]])
        return output, error

#####
# Parse out run/subrun/part given name
# My DB names are wrong:  Should be Run/Subrun/Part--tough to change now.
def parserun(name):
 if len(name) == 0:
   return [0, 0, 0]
 part1 = string.split(name, "Run")
 if len(part1) != 2:
   return [0, 0, 0]
 part2 = string.split(part1[1], "_")      # part2[0] = Run
 if len(part2) <= 2:
   return [0, 0, 0]
 part3 = string.split(part2[1], "Subrun") # part3[1] = SubrunUpper
 if len(part3) != 2:
   return [0, 0, 0]
 part4 = string.split(part2[2], ".")      # part4[0] = Subrun
 if len(part4) < 2:
   return [0, 0, 0]
 try:
   k = [int(part2[0]), int(part3[1]), int(part4[0])]
 except Exception:
   myprint(['SYTools.parserun failed to parse', name])
   k = [0, 0, 0]
 return k

############################################
######
# Determine if the file is forced, and if not does it exist already
# Return 0 if the file either does not exist yet or must be forced
#  to be reprocessed
# Return 1 if the file already exists and may be disposed of
def checkexistence(run, sru, sr, listforce, listsdst, listready):
 #
 if isinstance(listforce, list):
  if len(listforce) > 0:
   for word in listforce:
    if not isinstance(word, list):
      myprint(['SYTools.checkexistence was given a listforce with non-list contents'])
      sys.exit(1)
    if run == int(word[0]) and sru == int(word[1]) and sr == int(word[2]):
     return 0		# force it
 #
 if isinstance(listsdst, list):
  if len(listsdst) > 0:
   for word in listsdst:
    if not isinstance(word, list):
      myprint(['SYTools.checkexistence was given a listsdst with non-list contents'])
      sys.exit(1)
    if run == int(word[0]) and sru == int(word[1]) and sr == int(word[2]):
     return 1		# already processed
 #
 if isinstance(listready, list):
  if len(listready) > 0:
   for word in listready:
    if not isinstance(word, list):
      myprint(['SYTools.checkexistence was given a listready with non-list contents'])
      sys.exit(1)
    if run == int(word[0]) and sru == int(word[1]) and sr == int(word[2]):
     return 1		# already in the stack for processing
 #
 return 0	# not there, must be new.  Go for it.


########################################
#####
# Tar up things for disposal
def tarupRaw(tapedir):
  tarfile = tapedir + ".rawcheck.tar"
  outputlist = tapedir + ".outputlist"
  log = tapedir + '.log'
  jcp = 'JobCheckPartial.' + tapedir + '.submit'
  rcs = tapedir + '.rawchecksum.semaphore'
  cmd = ['tar', '--remove-files', '--ignore-failed-read', '-cf', tarfile, tapedir, outputlist, log, jcp, rcs]
  goterr = dosimplecommand(cmd)
  # Don't do the error handling, because tar Warns about missing files and the result acts like
  # a real error.
  #if goterr<0:
  #  myprint(['SYTools.tarupRaw failed during cleanup of ', tapedir])
  #  sys.exit(1)

######
#
def MakeSubmit(cfilename, ZBasefile, ZTapename, ZTotal):
 try:
  cfile = open(cfilename, 'w')
 except Exception:
  myprint(['MakeSubmit failed to create the submit file ' + cfilename])
  sys.exit(1)
 try:
  cfile.write("Jobname = jobcheckpartial\n")
  cfile.write("executable  =  jobcheckpartial\n")
  cfile.write("output          =  " + ZTapename + "/$(Jobname).$(Process).$(Cluster).out\n")
  cfile.write("error           =  " + ZTapename + "/$(Jobname).$(Process).$(Cluster).err\n")
  cfile.write("log             =  onejob.check.bulklogfile.log\n")
  cfile.write("getenv          =  true\n")
  cfile.write("universe        =  vanilla\n")
  cfile.write("notification    =  never\n")
  cfile.write("Arguments  =  " + ZBasefile + " $(Process) " + ZTapename + "\n")
  cfile.write("queue " + ZTotal + "\n")
  cfile.close()
  return
 except Exception:
  myprint(['MakeSubmit failed to write to the submit file ' + cfilename, sys.exc_info()[0]])
  sys.exit(1)

