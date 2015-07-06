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
/bin/sh $DIR/k.sh JHSBrandItemCheck python python

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jhs/brand_itemcheck/add_checkItems_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JHSBrandItemCheck.py $m_type > $LOGFILE

# process queue
p_num=3
obj='item'
crawl_type='check'
DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JHSWorkerM $obj $crawl_type

/usr/local/bin/python $DIR/JHSWorkerM.py $p_num $obj $crawl_type >> $LOGFILE


