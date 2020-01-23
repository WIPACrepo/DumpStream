#!/bin/bash
# arguments:
# $1 => real directory
# $2 => ideal directory
#IGNORE FOR NOW # $3 => condor job #

# 0'th step:  sanity!
#IGNORE FOR NOW if [[ "$2" == "" ]]; then echo "$0 needs directory and condor_job_id"; exit 1; fi
if [[ "$1" == "" ]]; then echo "$0 needs directory"; exit 1; fi
directory=$1
idealDirectory=$2
if [[ ! -d ${directory} ]]; then echo "$1 is not a visible directory"; exit 1; fi

# First curl to let the REST server know what we're doing with this directory. 
# The table in question is WorkingTable
# New status = Preparing
manglefile=$( echo "${idealDirectory}" | sed 's@/@+++@g' )
manglecom=$( echo "${manglefile}@Preparing" )
/usr/bin/curl -sS -X POST -H Content-Type:application/x-www-form-urlencoded http://archivecontrol.wipac.wisc.edu:80/glue/workupdate/${manglecom}
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ set Preparing"
    exit 1
  fi

# Second setup
cd ~jade/lta
source env/bin/activate

# Third do the load
echo ./ltacmd catalog load --path ${directory}
if [[ $? != 0 ]]
  then
     # try again
echo     ./ltacmd catalog load --path ${directory}
     if [[ $? != 0 ]]
       then
          echo "FAILURE with catalog load of ${directory}"
          exit 1
       fi
  fi

# Fourth setup the request

echo ./ltacmd request new --source WIPAC --dest NERSC --path ${directory}
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ request"
    exit 1
  fi

# Fifth curl to let the REST server know we are done with this directory
# New status = Picked
manglecom=$( echo "${manglefile}@Picked" )
/usr/bin/curl -sS -X POST -H Content-Type:application/x-www-form-urlencoded http://archivecontrol.wipac.wisc.edu:80/glue/workupdate/${manglecom}
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ set Picked"
    exit 1
  fi

exit 0
