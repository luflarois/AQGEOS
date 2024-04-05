#!/bin/bash
day=`date +%Y%m%d` #-d -1day |awk '{print$3}
echo $day
#
source /home/luiz.flavio/.bashrc
cd /home/luiz.flavio/scripts
./geos.py $day
