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
                JAVA_PID=`ps -C java -f --width 2000|grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else
                JAVA_PID=`ps -C java -f --width 2000|grep "$STR"|grep -v grep|awk '{print $2}'`
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

if [ "$polardbx" != "true" ]; then
    echo "Elastic rescale only support PolarDB-X mode"
    exit 1
fi

if [ x"$serverPort" == "x" ]; then
     serverPort=3306
fi

TEMP=`getopt -o p:m:c:hD -- "$@"`
eval set -- "$TEMP"
while true ; do
  case "$1" in
        -h) usage; shift ;;
        -p) serverPort=$2; shift 2;;
        -m) memory=$2; shift 2 ;;
        -c) cpu=$2; shift 2 ;;
        --) shift;;
        *)
            shift;
            if [ $# -eq 0 ]; then
                break;
            fi
            ;;
  esac
done

if [ -z "$memory" ] && [ -z "$cpu" ]; then
    #at least one argument is required
    echo "memory(-m) or cpu(-c) is required"
    exit 1
fi

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
            echo "PolarDB-X CN is not running."
        	  exit 1
        else
            echo "PolarDB-X CN is running, but with another pid."
            exit 1
        fi
    else
        echo "PolarDB-X CN is not running."
        exit 1
    fi
else
    #get pid by ps command instead of reading from pidfile
    pid=`ps -C java -f --width 2000|grep drds-server|grep 'DserverPort=${serverPort}'|awk '{print $2}'`
    if [ x"$other" == "x" ]; then
        args=`cat $pidfile | awk -F'#@#' '{print $2}'`
    fi

    if [ "$pid" == "" ] ; then
        pid=`get_pid "$name"`
    fi

    #check current process enables elasticRescale
    pid=`ps -C java -f --width 2000|grep ${pid}|grep "ElasticHeapMinHeapSize"|awk '{print $2}'`
    if [ x"$pid" == "x" ]; then
        echo "PolarDB-X CN is not running with elastic rescale"
        exit 1
    fi

    if ps -C java -f --width 2000|grep drds-server|grep ${pid}|grep -q "UseWisp2"; then
        echo "Elastic rescale does not support Wisp coroutine"
        exit 1
    fi

    echo -e "`hostname`: Online elastic rescale PolarDB-X CN: $pid ... "

    #1. skip checking whether target memory and cores are available. just do the rescale
    #2. skip checking whether target value is the same as current value
    #3. no need to retry if failed

    if [ -z "$memory" ]; then
        echo "Rescaling cores to: ${cpu}C"
        output=$(jcmd $pid GC.elastic_heap parallel_gc_threads=$cpu 2>&1)
        if echo "$output" | grep -q "succeed"; then
            echo "Successfully rescale cores to: $cpu"
            exit 0
        else
            echo "Failed to rescale cores to: $cpu"
            echo "$output"
            exit 1
        fi
        exit 0
    fi

    if [ -z "$cpu" ]; then
        echo "Rescaling memory to: ${memory}G"
        if [ $memory -eq 128 ] ; then
            heapOpts="soft_min_heap_size=110G soft_max_heap_size=110G"
        elif [ $memory -eq 64 ] ; then
            heapOpts="soft_min_heap_size=50G soft_max_heap_size=50G"
        elif [ $memory -eq 32  ] ; then
            heapOpts="soft_min_heap_size=24G soft_max_heap_size=24G"
        elif [ $memory -eq 16  ] ; then
            heapOpts="soft_min_heap_size=10G soft_max_heap_size=10G"
        elif [ $memory -eq 8 ] ; then
            heapOpts="soft_min_heap_size=4G soft_max_heap_size=4G"
        elif [ $memory -eq 4 ] ; then
            heapOpts="soft_min_heap_size=2G soft_max_heap_size=2G"
        else
            echo "Do not support rescaling memory to: ${memory}G, only supports 4/8/16/32/64/128G currently"
            exit 1
        fi

        output=$(jcmd $pid GC.elastic_heap $heapOpts 2>&1)
        if echo "$output" | grep -q "succeed"; then
            echo "Successfully rescale memory to: ${memory}G"
            exit 0
        else
            echo "Failed to rescale memory to: ${memory}G"
            echo "$output"
            exit 1
        fi
        exit 0
    fi

    echo "Rescaling to: ${cpu}C${memory}G"
    if [ $memory -eq 128 ] ; then
        heapOpts="soft_min_heap_size=110G soft_max_heap_size=110G"
    elif [ $memory -eq 64 ] ; then
        heapOpts="soft_min_heap_size=50G soft_max_heap_size=50G"
    elif [ $memory -eq 32  ] ; then
        heapOpts="soft_min_heap_size=24G soft_max_heap_size=24G"
    elif [ $memory -eq 16  ] ; then
        heapOpts="soft_min_heap_size=10G soft_max_heap_size=10G"
    elif [ $memory -eq 8 ] ; then
        heapOpts="soft_min_heap_size=4G soft_max_heap_size=4G"
    elif [ $memory -eq 4 ] ; then
        heapOpts="soft_min_heap_size=2G soft_max_heap_size=2G"
    else
        echo "Do not support rescaling memory to: ${memory}G, only supports 4/8/16/32/64/128G currently"
        exit 1
    fi

    output=$(jcmd $pid GC.elastic_heap $heapOpts parallel_gc_threads=$cpu 2>&1)
    if echo "$output" | grep -q "succeed"; then
        echo "Successfully rescale to: ${cpu}C${memory}G"
        exit 0
    else
        echo "Failed to rescale memory to: ${cpu}C${memory}G"
        echo "$output"
        exit 1
    fi
    exit 0
fi