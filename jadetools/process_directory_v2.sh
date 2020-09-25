#!/bin/bash
# process_directory_v2.sh IdealDirectoryName DB_director_key
# ASSUMPTION:
#  The directory may be a link to the real one, but it must be in the
#  canonical position in our data warehouse.
#
# Arguments:
# $1 => directory.  This is /data/exp etc, and may be a link to the real
#       directory elsewhere, but this is the way we must reference it
# $2 => dirkey of this FullDirectory entry
#
#
hostname
if [[ "$1" == "" ]]; then echo "$0 needs directory and the DB key for this row"; exit 1; fi
if [[ "$2" == "" ]]; then echo "$0 needs a directory and the DB key for this row"; exit 1; fi
directory=$1
dirkey=$2
if [[ ! -d ${directory} ]]; then echo "${directory} is not a visible directory"; exit 1; fi
#
##
# The first REST server connection is done by the main program before submitting this job
# The table in question is FullDirectory
idate=$( date +%s )
echo "process_directory_v2.sh ENTRANCE DEBUG $* ${idate}"

##
# First execute the file catalog loading script
if ! env -i ./load_filecatalog "$1"
   then
      ldate=$( date +%s )
      echo "Giving up trying to load $1 into the file catalog, ${ldate}"
      exit 1
   fi 

##
# Second setup the request
alls=$(env -i ./tell_bundlemaker "$1")
echo "${alls}" | grep "$1" >& /dev/null
if [[ $? != 0 ]]
   then
      tdate=$( date +%s )
      echo "Giving up trying to tell the bundler about $1, ${tdate}"
      echo "${alls}"
      exit 2
   fi
ltaid=$(echo "${alls}" | awk '{print $1;}')

##
# Lastly, curl to let the REST server know we are done with this directory
if [[ -f /usr/bin/curl ]]
  then
    CURL=/usr/bin/curl
  else
    CURL=/bin/curl
  fi
cdate=$( date +%s )
manglecom=$( "${dirkey}@LTArequest@${ltaid}" )
CURLARGS="-sS -X POST -H Content-Type:application/x-www-form-urlencoded"
target="http://archivecontrol.wipac.wisc.edu/directory/modify/${manglecom}"
if ! ${CURL} ${CURLARGS} "${target}"
  then
    echo "FAILURE w/ set status to LTArequest ${cdate}"
    exit 5
  fi

exit 0
