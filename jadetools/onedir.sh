#!/bin/bash
# arguments:
# $1 => directory
# $2 => condor job #

# 0'th step:  sanity!
if [[ "$2" == "" ]]; then echo "$0 needs directory and condor_job_id"; exit 1; fi
directory=$1
if [[ ! -d ${directory} ]]; then echo "$1 is not a visible directory"; exit 1; fi

# First curl to let the REST server know what we're doing with this directory
# New status = Preparing

# Second setup
cd ~jade/lta
source env/bin/activate

# Third do the load
./ltacmd catalog load --path ${directory}
if [[ $? != 0 ]]
  then
     # try again
     ./ltacmd catalog load --path ${directory}
     if [[ $? != 0 ]]
       then
          echo "FAILURE with catalog load of ${directory}"
          exit 1
       fi
  fi

# Fourth setup the request

./ltacmd request new --source WIPAC --dest NERSC --path ${directory}

# Fifth curl to let the REST server know we are done with this directory
# New status = Loaded

exit 0
