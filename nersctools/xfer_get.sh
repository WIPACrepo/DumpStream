BATCH -M escori
#SBATCH -q xfer
#SBATCH -t 12:00:00




if [[ "$2" != "" ]]
  then
    TAPEPATH=$2
  else
    TAPEPATH='/home/projects/icecube'
  fi

STAGEPATH='/global/cscratch1/sd/icecubed/read-disk'
EXPORTPATH='/global/cscratch1/sd/icecubed/export-disk'


dataexp_file=`echo $1 |sed s@$TAPEPATH@@g`
# Then mkdir on tape, copy file and checksum
dir=`dirname $dataexp_file`


echo "START" `date`

if [ -e ${EXPORTPATH}/$dataexp_file ]; then
 echo FILE ALREADY ON DISK 
 ls -l ${EXPORTPATH}/$dataexp_file
 exit
fi

echo mkdir -p ${STAGEPATH}/$dir
mkdir -p ${STAGEPATH}/$dir

echo get ${STAGEPATH}/$dataexp_file : $1 
/usr/common/mss/bin/hsi -q get ${STAGEPATH}/$dataexp_file : $1

echo mkdir -p ${EXPORTPATH}/$dir 
mkdir -p ${EXPORTPATH}/$dir

echo mv ${STAGEPATH}/$dataexp_file ${EXPORTPATH}/$dataexp_file
mv ${STAGEPATH}/$dataexp_file ${EXPORTPATH}/$dataexp_file


echo "END" `date`
