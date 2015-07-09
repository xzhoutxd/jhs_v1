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
/bin/sh $DIR/k.sh JHSBrandPosition python python

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand_position/add_catposition_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSBrandPosition.py $m_type > $LOGFILE

# process queue
p_num=4
obj='act'
crawl_type='position'
DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JHSWorkerM $obj $crawl_type

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand_position/add_actposition_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSWorkerM.py $p_num $obj $crawl_type > $LOGFILE

