#!/bin/bash
# ASSUMPTION:
# The argument is a real directory, in a tree modeled on our file catalog
#  This directory may be a link to the real one, but it must be in the
#  canonical position in our data warehouse.
#
# Arguments:
# $1 => directory.  This is /data/exp etc, and may be a link to the real
#       directory elsewhere, but this is the way we must reference it
if [[ "$1" == "" ]]; then echo "$0 needs directory"; exit 1; fi
directory=$1
if [[ ! -d ${directory} ]]; then echo "${directory} is not a visible directory"; exit 1; fi

# First curl to let the REST server know what we're doing with this directory. 
# The table in question is WorkingTable
# New status = Preparing
manglefile=$( echo "${directory}" | sed 's@/@+++@g' )
manglecom=$( echo "${manglefile}@Preparing" )
/usr/bin/curl -sS -X POST -H Content-Type:application/x-www-form-urlencoded  http://archivecontrol.wipac.wisc.edu:80/glue/workupdate/"${manglecom}"
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ set Preparing"
    exit 2
  fi

# Second execute the file catalog loading script
if ! ./load_filecatalog "$1"
   then
      echo "Giving up trying to load $1 into the file catalog"
      exit 1
   fi 

# Third setup the request

if ! ./tell_bundlemaker "$1"
   then
      echo "Giving up trying to the bundler about $1"
      exit 2
   fi 

# Fourth curl to let the REST server know we are done with this directory
# New status = Picked
manglecom=$( echo "${manglefile}@Picked" )
/usr/bin/curl -sS -X POST -H Content-Type:application/x-www-form-urlencoded  http://archivecontrol.wipac.wisc.edu:80/glue/workupdate/"${manglecom}"
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ set Picked"
    exit 5
  fi

exit 0
