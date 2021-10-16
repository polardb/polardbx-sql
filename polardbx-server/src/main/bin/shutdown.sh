#!/bin/bash

cygwin=false;
linux=false;
case "`uname`" in
    CYGWIN*)
        bin_abs_path=`cd $(dirname $0); pwd`
        cygwin=true
        ;;
    Linux*)
        bin_abs_path=$(readlink -f $(dirname $0))
        linux=true
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac

get_pid() { 
    STR=$1
    PID=$2
    if $cygwin; then
        JAVA_CMD="$JAVA_HOME\bin\java"
        JAVA_CMD=`cygpath --path --unix $JAVA_CMD`
        JAVA_PID=`ps |grep $JAVA_CMD |awk '{print $1}'`
    else
        if $linux; then
            if [ ! -z "$PID" ]; then
                JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else 
                JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep -v grep|awk '{print $2}'`
            fi
        else
            if [ ! -z "$PID" ]; then
                JAVA_PID=`ps aux |grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else 
                JAVA_PID=`ps aux |grep "$STR"|grep -v grep|awk '{print $2}'`
            fi
        fi
    fi
    echo $JAVA_PID;
}

base=${bin_abs_path}/..
serverPort=3306
debugPort=
other=
#eth0=`ifconfig bond0 | grep --word-regexp inet | awk '{print $2}' | cut -d":" -f2`
eth0="0.0.0.0"
polardbx=false

# load env for polardbx by server_env.sh
source /etc/profile
if [ -f /home/admin/bin/server_env.sh ] ; then
     source /home/admin/bin/server_env.sh >> /dev/null 2>&1
fi

# try to load metadb env by config.properties
config="/home/admin/drds-server/env/config.properties"
if [ -f "$config" ]
then
	echo "metaDb env config found: $config"
	while IFS='=' read -r key value
	do
		## '.' replace as '-'
		key=$(echo $key | tr '.' '_')
		## ignore the comment of properties
		[[ -z $(echo "$key" | grep -P '\s*#+.*' ) ]] \
			&& eval "${key}='${value}'"
	done < "$config"
else
	echo "metaDb env config not found: $config, then use server_env.sh instead."
fi

# check polardbx mode
if [ x"$metaDbAddr" != "x" ]; then
     polardbx=true
fi

TEMP=`getopt -o d:p:m:c:a:f:i:s:hD -- "$@"`
eval set -- "$TEMP"
while true ; do
  case "$1" in
        -h) usage; shift ;;
        -D) other=1; shift ;;
        -p) serverPort=$2; shift 2;;
        -m) other=1; shift 2 ;;
        -d) debugPort=$2; shift 2;;
        -i) other=1; shift 2 ;;
        -c) other=1; shift 2 ;;
        -f) other=1; shift 2 ;;
        -s) other=1; shift 2 ;;
        -a) other=1; shift 2;;
        --) shift;;
        *)  
            shift;
            if [ $# -eq 0 ]; then
                break;
            fi
            ;; 
  esac
done

pidfile=$base/bin/tddl.pid
name="appName=tddl"
if [ x"$serverPort" != "x" ]; then
    pidfile=$base/bin/tddl_${serverPort}.pid
    if [ x"$polardbx" == "xtrue" ]; then
        pidfile=$base/bin/tddl.pid
    fi
    name="appName=tddl_$serverPort"
fi

if [ ! -f "$pidfile" ];then
    if [ x$serverPort != "x" ]; then
        running=`netstat -tanp 2>/dev/null | grep java |grep "${eth0}:${serverPort} " | awk '{print $NF}' | sort | uniq | wc -l`
        if [ $running -eq 0 ]; then
            echo "tddl is not running. exists"
        	exit 0
        else
            echo "tddl is running, but with another pid. "
            exit 1
        fi
    else
        echo "tddl is not running. exists"
        exit 0
    fi
else
    #get pid by ps command instead of reading from pidfile
    pid=`ps -C java -f --width 1000|grep drds-server|grep "DserverPort=${serverPort}" |awk '{print $2}'`
    if [ x"$other" == "x" ]; then
        args=`cat $pidfile | awk -F'#@#' '{print $2}'`
    fi
    if [ x"$debugPort" != "x" ]; then
        args="$args -d $debugPort"
    fi
    
    if [ "$pid" == "" ] ; then
        pid=`get_pid "$name"`
    fi
    
    echo -e "`hostname`: stopping tddl $pid ... "
    kill $pid
    
    cost=0
    timeout=40
    while [ $timeout -gt 0 ]; do 
        gpid=`get_pid "$name" "$pid"`
        if [ "$gpid" == "" ] ; then
            running=`netstat -tanp 2>/dev/null | grep java |grep "${eth0}:${serverPort} " | awk '{print $NF}' | sort | uniq | wc -l`
            if [ "$running" == "0" ]; then
                echo "Oook! cost:$cost"
                `rm -rf $pidfile`
                break;
            else
                echo "Check if current running pid differ from the one in pid file."
                exit 1
            fi
        fi
        sleep 1
        let timeout=timeout-1
        let cost=cost+1
    done
    
    if [ $timeout -eq 0 ] ; then
        kill -9 $pid
        gpid=`get_pid "$name" "$pid"`
        if [ "$gpid" == "" ] ; then
            running=`netstat -tanp 2>/dev/null | grep java |grep "${eth0}:${serverPort} " | awk '{print $NF}' | sort | uniq | wc -l`
            if [ "$running" == "0" ]; then
                echo "Oook! cost:$cost"
                `rm -rf $pidfile`
                break;
            else
                echo "Check if current running pid differ from the one in pid file."
                exit 1
            fi
        else
            echo "Check kill pid ${pid} failed."
            exit 1
        fi
    fi
fi
