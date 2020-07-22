#!/bin/bash
# process_tree.sh IdealDirectoryName
# ASSUMPTION:
#  The directory may be a link to the real one, but it must be in the
#  canonical position in our data warehouse.
#
# Arguments:
# $1 => directory.  This is /data/exp etc, and may be a link to the real
#       directory elsewhere, but this is the way we must reference it
#       We descend the whole tree, for all days
#       Since this has nothing to do with the dumper, the dump's REST
#       server is ignored
#
#
hostname
if [[ "$1" == "" ]]; then echo "$0 needs directory"; exit 1; fi
directory=$1
if [[ ! -d ${directory} ]]; then echo "${directory} is not a visible directory"; exit 1; fi
#
##
# The table in question is FullDirectory
echo "process_tree.sh ENTRANCE DEBUG $0"

##
# First execute the file catalog loading script
if ! env -i ./load_filecatalog "$1"
   then
      echo "Giving up trying to load $1 into the file catalog"
      exit 1
   fi 

##
# Second setup the request
alls=$(env -i ./tell_bundlemaker "$1")
echo "${alls}" | grep "$1" >& /dev/null
if [[ $? != 0 ]]
   then
      echo "Giving up trying to tell the bundler about $1"
      echo "${alls}"
      exit 2
   fi
#ltaid=$(echo "${alls}" | awk '{print $1;}')

exit 0
