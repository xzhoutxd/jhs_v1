#!/bin/sh

DATESTR=`date +"%Y%m%d%H"`

DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JHSActPosition python python

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand_position/add_Actsposition_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSActPosition.py > $LOGFILE

