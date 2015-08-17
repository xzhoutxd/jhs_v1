#!/bin/sh

DATESTR=`date +"%Y%m%d%H"`

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
/bin/sh $DIR/k.sh JHSBrand python python

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand/add_cat_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSBrand.py $m_type > $LOGFILE

# process queue
p_num=3
obj='act'
crawl_type='main'
DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JHSWorkerM $obj $crawl_type

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand/add_brands_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSWorkerM.py $p_num $obj $crawl_type > $LOGFILE

