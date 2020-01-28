#!/bin/bash
# arguments:
# $1 => real directory
#IGNORE FOR NOW # $2 => condor job #
# 0'th step:  sanity!
#IGNORE FOR NOW if [[ "$2" == "" ]]; then echo "$0 needs directory and condor_job_id"; exit 1; fi
if [[ "$1" == "" ]]; then echo "$0 needs directory"; exit 1; fi
directory=$1
if [[ ! -d ${directory} ]]; then echo "${directory} is not a visible directory"; exit 1; fi

# First curl to let the REST server know what we're doing with this directory. 
# The table in question is WorkingTable
# New status = Preparing
manglefile=$( echo "${directory}" | sed 's@/@+++@g' )
manglecom=$( echo "${manglefile}@Preparing" )
/usr/bin/curl -sS -X POST -H Content-Type:application/x-www-form-urlencoded -H "Authorization: Token "`cat /home/jbellinger/archiver/DumpStream/jadetools/token`\" http://archivecontrol.wipac.wisc.edu:80/glue/workupdate/${manglecom}
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ set Preparing"
    exit 2
  fi

# Second setup
cd ~jade/lta
source env/bin/activate

# Third do the load
./ltacmdjnb catalog load --path ${directory}
if [[ $? != 0 ]]
  then
     # try again
     ./ltacmdjnb catalog load --path ${directory}
     if [[ $? != 0 ]]
       then
          echo "FAILURE with catalog load of ${directory}"
          exit 3
       fi
  fi

# Fourth setup the request

./ltacmdjnb request new --source WIPAC --dest NERSC --path ${directory}
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ request"
    exit 4
  fi

# Fifth curl to let the REST server know we are done with this directory
# New status = Picked
manglecom=$( echo "${manglefile}@Picked" )
/usr/bin/curl -sS -X POST -H Content-Type:application/x-www-form-urlencoded -H "Authorization: Token "`cat /home/jbellinger/archiver/DumpStream/jadetools/token`\" http://archivecontrol.wipac.wisc.edu:80/glue/workupdate/${manglecom}
if [[ $? != 0 ]]
  then
    echo "FAILURE w/ set Picked"
    exit 5
  fi

exit 0
