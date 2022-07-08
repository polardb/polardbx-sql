#!/bin/bash 
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
args=$@
. ${base}/bin/shutdown.sh $args 
if [ $? == 0 ]; then
    . ${base}/bin/startup.sh $args
else
    echo "Stop failed , manually check needed."
fi
