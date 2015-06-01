#!/bin/sh

DATESTR=`date +"%Y%m%d%H"`

if [ $# = 0 ]; then
    echo " Usage: $0 master|slave" 
    echo " e.g. : $0 m|s" 
    exit 1
else
    m_type=$1
fi
DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JHSBrandUpdate

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand_update/add_updateBrands_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSBrandUpdate.py $m_type > $LOGFILE

