#!/bin/bash
#SBATCH -M escori
#SBATCH -q xfer
#SBATCH -t 12:00:00

#DATE=`date +%Y%m%d-%H%M`

# First strip out the disk buffer pre-path
DISKPATH='/global/cscratch1/sd/icecubed/jade-disk/'

dataexp_file=`echo $1 |sed s@$DISKPATH@@g`
if [[ "$2" != "" ]]
  then
    TAPEPATH=$2
  else
    TAPEPATH=/home/projects/icecube
  fi

# Then mkdir on tape, copy file and checksum
dir=`dirname $dataexp_file`

echo "START" `date`

echo mkdir ${TAPEPATH}/${dir}
#echo mkdir /home/projects/icecube/$dir
#/usr/common/mss/bin/hsi -q mkdir /home/projects/icecube/$dir 
/usr/common/mss/bin/hsi -q mkdir -p ${TAPEPATH}/${dir} 

#echo put $1 : /home/projects/icecube/$dataexp_file 
echo put $1 : ${TAPEPATH}/$dataexp_file 

#/usr/common/mss/bin/hsi -q put -c on -H sha512 $1 : /home/projects/icecube/$dataexp_file 
/usr/common/mss/bin/hsi -q put -c on -H sha512 $1 : ${TAPEPATH}/$dataexp_file 

#if [[ "$size" != "" ]]
#then
#/usr/common/mss/bin/hsi -q ls -l /home/projects/icecube/$dataexp_file
/usr/common/mss/bin/hsi -q ls -l ${TAPEPATH}/$dataexp_file
#  fi

echo "END" `date`
