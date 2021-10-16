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
for pid in `ls -al ${base}/bin/ | grep pid | awk '{print $9}' | tr -s '\n' ' '` ; do
	args=`cat $pid | awk -F'#@#' '{print $2}'` 
	${base}/bin/shutdown.sh $args
	${base}/bin/startup.sh $args
done