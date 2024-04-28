#!/bin/bash

function usage() {
        echo "About"
        echo "  This is manage script. You can imitate manage server through startup.sh command."
        echo "Usage:"
        echo "  startup.sh [<options>] [args]"
        echo "Options: "
        echo "  -I              Initialize GMS"
        echo "  -r rootPassword MetaDB Root password"
        echo "  -u polarxUser   PolarDB-X root user"
        echo "  -S polarxPasswd PolarDB-X root password"
        echo "  -P dnPasswdKey  DnPasswordKey"
        echo "  -d dnList       dnList"
        echo "  -F              Force cleanup before initialize"
        echo "  -h              Show Help"
        echo "  -D				      DRDS mode"
        echo "  -b port			    enable debug port"
        echo "  -q mock         fast mock mode"
        echo "  -p port			    server port"
        echo "  -m port			    manage port"
        echo "  -c cluster		  server cluster name"
        echo "	-s instanceId	  server instance id"
        echo "	-i idc			    server idc name"
        echo "	-w wisp			    enable jvm wisp"
        echo "  -e engine       the engine of cold data(OSS or LOCAL_DISK)"
        echo "	--uri uri		    uri of cold file"
        echo "	--ep endpoint		endpoint of oss"
        echo "	--ak accessKey 	access key ID of oss"
        echo "	--sk secretKey	access key secret of oss"
        echo "  -f conf			    conf file, Default conf file is $BASE/conf/server.properties"
        echo "  -a key=value[;] args config, Example: -a k1=v1;k2=v2"
        echo "Examples:"
        echo "1. startup.sh -b 8080"
        echo "      startup with debug port 8080"
        echo "2. startup.sh -p 8507 -c cluster"
        echo "      startup with serverport 8507 and cluster"
        echo "3. startup.sh -I -P 855a7043dfcb1f9a -d 127.0.0.1:4886"
        echo "      initialize cluster"
        exit 0;
}

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
export LANG=en_US.UTF-8
export BASE=$base

args=$@
serverPort=
managerPort=
debugPort=
cluster=
serverArgs=
instanceId=
loggerRoot=
idc=
wisp=
drds=
mockDRDS=
mockAppName=
polardbx=false
fast_mock=
initializeGms=false
vDnPasswordKey=
dnList=
rootPasswd=
polarxRootUser=
engine=
uri=
endpoint=
accessKey=
secretKey=
polarxRootPasswd=
forceCleanup=false

tddl_conf=$base/conf/server.properties
logback_configurationFile=$base/conf/logback.xml
base_log=$base/logs/tddl
pidfile=$base/bin/tddl.pid
KERNEL_VERSION=`uname -r`
enable_bianque=false

checkuser=`whoami`
if [ x"$checkuser" = x"root" ];then
   echo "Can not execute under root user!";
   exit 1;
fi

TEMP=`getopt -o q:d:b:p:l:m:c:a:f:i:w:s:r:u:S:A:P:e:hDMIF --long uri:,ep:,ak:,sk: -- "$@"`
eval set -- "$TEMP"
while true ; do
  case "$1" in
        -h) usage; shift ;;
        -I) initializeGms=true; shift ;;
        -P) vDnPasswordKey=$2; shift 2;;
        -r) rootPasswd=$2; shift 2;;
        -u) polarxRootUser=$2; shift 2;;
        -S) polarxRootPasswd=$2; shift 2;;
        -F) forceCleanup=true; shift ;;
        -d) dnList=$2; shift 2;;
        -D) drds=true; shift ;;
        -M) mockDRDS=true; shift ;;
        -A) mockAppName=$2; shift 2;;
        -p) serverPort=$2; shift 2;;
        -l) loggerRoot=$2; shift 2;;
        -m) managerPort=$2; shift 2 ;;
        -b) debugPort=$2; shift 2;;
        -q) fast_mock=$2; shift 2;;
        -e) engine=$2; shift 2;;
        --uri) uri=$2; shift 2;;
        --ep) endpoint=$2; shift 2;;
        --ak) accessKey=$2; shift 2;;
        --sk) secretKey=$2; shift 2;;
        -i) idc=`echo $2|sed "s/'//g"`; shift 2 ;;
        -w) wisp=`echo $2|sed "s/'//g"`; shift 2 ;;
        -c) cluster=`echo $2|sed "s/'//g"`; shift 2 ;;
        -f) tddl_conf=`echo $2|sed "s/'//g"`; shift 2 ;;
        -s) instanceId=`echo $2|sed "s/'//g"`; shift 2 ;;
        -a)
        	config=`echo $2|sed "s/'//g"`
        	if [ "$serverArgs" == "" ]; then
        		serverArgs="$config"
        	else
				serverArgs="$serverArgs;$config"
        	fi
        	shift 2;;
        --) shift;;
        *)
            shift;
            if [ $# -eq 0 ]; then
                break;
            fi
            ;;
  esac
done

if [[ -n $engine ]]; then
  if [[ "$engine" == "LOCAL_DISK" ]]; then
    oss=0
    if ! [[ -n $uri ]]; then
      echo "-uri should be set!"
      exit
    fi
  else
    if [[ "$engine" == "OSS" ]]; then
      oss=0
      if [[ -n $uri ]]; then
        oss=$((oss+1))
      fi
      if [[ -n $endpoint ]]; then
          oss=$((oss+1))
      fi
      if [[ -n $accessKey ]]; then
        oss=$((oss+1))
      fi
      if [[ -n $secretKey ]]; then
        oss=$((oss+1))
      fi
      if ! [[ $oss -eq 4 ]];then
        echo "-uri -ep -ak -sk should be set at the same time!"
        exit
      fi
    else
      echo "-e should be OSS or LOCAL_DISK, but is $engine!"
      exit
    fi
  fi
fi

source /etc/profile
# load env for polardbx by server_env.sh
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
			&& export "${key}=${value}"
	done < "$config"
else
	echo "metaDb env config not found: $config, then use server_env.sh instead."
fi

if [ x"$debugPort" != "x" ]; then
	DEBUG_SUSPEND="n"
	JAVA_DEBUG_OPT="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=$debugPort,server=y,suspend=$DEBUG_SUSPEND"
fi

if [ x"$initializeGms" != "x" ]; then
  TDDL_OPTS="$TDDL_OPTS -DinitializeGms=$initializeGms"
fi

if [ x"$forceCleanup" != "x" ]; then
  TDDL_OPTS="$TDDL_OPTS -DforceCleanup=$forceCleanup"
fi

if [ x"$vDnPasswordKey" != "x" ]; then
  export dnPasswordKey=$vDnPasswordKey
fi

if [ x"$dnList" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -DdnList=$dnList"
fi

if [ x"$rootPasswd" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -DrootPasswd=$rootPasswd"
fi

if [ x"$polarxRootUser" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -DpolarxRootUser=$polarxRootUser"
fi
if [ x"$polarxRootPasswd" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -DpolarxRootPasswd=$polarxRootPasswd"
fi

if [ x"$drds" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -Ddrds=$drds"
fi

if [ x"$mockDRDS" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -DmockDRDS=$mockDRDS"
fi

if [ x"$mockAppName" != "x" ]; then
  TDDL_OPTS=" $TDDL_OPTS -DmockAppName=$mockAppName"
fi

if [ x"$metaDbAddr" != "x" ]; then
	polardbx=true
	TDDL_OPTS=" $TDDL_OPTS -DmetaDbAddr=$metaDbAddr"
fi

if [ x"$metaDbName" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DmetaDbName=$metaDbName"
fi

if [ x"$metaDbProp" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DmetaDbProp=$metaDbProp"
fi

if [ x"$metaDbUser" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DmetaDbUser=$metaDbUser"
fi

if [ x"$metaDbPasswd" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DmetaDbPasswd=$metaDbPasswd"
fi

if [ x"$fast_mock" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -Dtddl.config.mode=fast_mock"
fi

if [ x"$serverPort" != "x" ]; then
    TDDL_OPTS=" $TDDL_OPTS -DappName=tddl_$serverPort -DserverPort=$serverPort"
    base_log=$base/$serverPort/logs/tddl

	if [ x"$polardbx" == "xtrue" ]; then
		base_log=$base/logs/tddl
		pidfile=$base/bin/tddl.pid
		TDDL_OPTS=" $TDDL_OPTS -DloggerRoot=.."
	else
		pidfile=$base/bin/tddl_${serverPort}.pid
		TDDL_OPTS=" $TDDL_OPTS -DloggerRoot=../$serverPort"
	fi

	if [ x"$managerPort" == "x" ]; then
		let managerPort=serverPort+100
	fi
else
	TDDL_OPTS=" $TDDL_OPTS -DappName=tddl"
fi

if [ x"$managerPort" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DmanagerPort=$managerPort"
fi

if [ x"$loggerRoot" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DloggerRoot=$loggerRoot"
fi

if [ x"$instId" != "x" ] && [ x"$instanceId" == "x" ]; then
	instanceId=$instId
fi

if [ x"$cluster" != "x" ] && [ x"$instanceId" == "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -Dcluster=$cluster"
fi

if [ x"$serverArgs" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DserverArgs=$serverArgs"
fi

if [ x"$instanceId" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DinstanceId=$instanceId"
fi

if [ x"$idc" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -Didc=$idc"
fi

if [ x"$engine" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -Dengine=$engine"
fi

if [ x"$uri" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -Duri=$uri"
fi

if [ x"$endpoint" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -Dendpoint=$endpoint"
fi

if [ x"$accessKey" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DaccessKey=$accessKey"
fi

if [ x"$secretKey" != "x" ]; then
	TDDL_OPTS=" $TDDL_OPTS -DsecretKey=$secretKey"
fi


if [ x"$enable_bianque" == "xtrue" ]; then
  BIANQUE_LIB_FILEPATH="/home/admin/bianquejavaagent/output/lib/libjava_bianque_agent.so"
  BIANQUE_SERVER_PORT=9874
  timeout 1 bash -c "nc -vw 1 127.0.0.1 $BIANQUE_SERVER_PORT 2>/tmp/bianquenc.log 1>/dev/null"
  connectedCnt=`cat /tmp/bianquenc.log | grep Connected | wc -l`
  if [ -f $BIANQUE_LIB_FILEPATH ] && [ $connectedCnt -gt 0 ] ; then
	  BIANQUE_AGENT_OPTS=" -agentpath:$BIANQUE_LIB_FILEPATH=local_path=/home/admin/bianquejavaagent/output"
  fi
fi

if [ ! -d $base_log ] ; then
	mkdir -p $base_log
fi

## set java path
TAOBAO_JAVA="/opt/taobao/java_coroutine/bin/java"
ALIBABA_JAVA="/usr/alibaba/java/bin/java"
DRAGONWELL_JAVA="/opt/java/dragonwell/bin/java"
if [ -f $TAOBAO_JAVA ] ; then
	JAVA=$TAOBAO_JAVA
	JGROUP="/opt/taobao/java_coroutine/bin/jgroup"
elif [ -f $ALIBABA_JAVA ] ; then
	JAVA=$ALIBABA_JAVA
elif [ -f $DRAGONWELL_JAVA ] ; then
  JAVA=$DRAGONWELL_JAVA
else
	JAVA=$(which java)
	if [ ! -f $JAVA ]; then
		echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
		exit 1
	fi
	JGROUP=$(which jgroup)
fi

if [ -f $pidfile ] ; then
  if [[ -n $engine ]]; then
    echo "init file storage"
  else
    echo "found $pidfile , Please run shutdown.sh first ,then startup.sh" 2>&2
    exit 1
  fi
fi

JavaVersion=`$JAVA -version 2>&1 |awk -F "version" '{print $2}' | sed -e '/^$/d' -e 's/"//g' | awk 'NR==1{ gsub(/"/,""); print $1}' | awk -F "." '{print $1}'`

if [ -f $pidfile ] ; then
	echo "found $pidfile , Please run shutdown.sh first ,then startup.sh" 2>&2
    exit 1
fi

str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
    freecount=`free -m | grep 'Mem' |awk '{print $2}'`

    if [ x"$mem_size" != "x" ]; then
        freecount=$mem_size
    fi

    if [ x"$memory" != "x" ]; then
        freecount=`expr $memory / 1024 / 1024`
    fi

    if [ $freecount -ge 131072 ] ; then
        JAVA_OPTS="-server -Xms110g -Xmx110g -XX:MaxDirectMemorySize=16g"
    elif [ $freecount -ge 65536 ] ; then
        JAVA_OPTS="-server -Xms50g -Xmx50g -XX:MaxDirectMemorySize=12g"
    elif [ $freecount -ge 32768  ] ; then
        JAVA_OPTS="-server -Xms24g -Xmx24g -XX:MaxDirectMemorySize=6g"
    elif [ $freecount -ge 16384  ] ; then
        JAVA_OPTS="-server -Xms10g -Xmx10g -XX:MaxDirectMemorySize=3g"
    elif [ $freecount -ge 8192 ] ; then
        JAVA_OPTS="-server -Xms4g -Xmx4g "
    elif [ $freecount -ge 4096 ] ; then
        JAVA_OPTS="-server -Xms2g -Xmx2g "
    elif [ $freecount -ge 2048 ] ; then
        JAVA_OPTS="-server -Xms1024m -Xmx1024m "
    elif [ $freecount -ge 1024 ] ; then
        JAVA_OPTS="-server -Xms512m -Xmx512m "
    elif [ $freecount -ge 512 ] ; then
        JAVA_OPTS="-server -Xms256m -Xmx256m "
    elif [ $freecount -ge 256 ] ; then
        JAVA_OPTS="-server -Xms128m -Xmx128m "
    fi
else
	echo "not support 32-bit java startup"
	exit
fi

#2.6.32-220.23.2.al.ali1.1.alios6.x86_64 not support Wisp2
if [ "$wisp" == "wisp" ] && [ "$KERNEL_VERSION" != "2.6.32-220.23.2.al.ali1.1.alios6.x86_64" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseWisp2 -Dio.grpc.netty.shaded.io.netty.transport.noNative=true -Dio.netty.transport.noNative=true"
fi

# in docker container, limit cpu cores
if [ x"$cpu_cores" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:ActiveProcessorCount=$cpu_cores"
fi

#https://workitem.aone.alibaba-inc.com/req/33334239
JAVA_OPTS="$JAVA_OPTS -Dtxc.vip.skip=true "

JAVA_OPTS="$JAVA_OPTS -Xss4m -XX:+AggressiveOpts -XX:-UseBiasedLocking -XX:-OmitStackTraceInFastThrow "

if [ $JavaVersion -ge 11 ] ; then
  JAVA_OPTS="$JAVA_OPTS"
else
  JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods"
fi

# For CMS and ParNew
#JAVA_OPTS="$JAVA_OPTS -XX:SurvivorRatio=10 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=75"
# For G1
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent "

if [ $JavaVersion -ge 11 ] ; then
  JAVA_OPTS="$JAVA_OPTS"
else
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintAdaptiveSizePolicy -XX:+PrintTenuringDistribution"
fi

export LD_LIBRARY_PATH=../lib/native

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Dcom.alibaba.java.net.VTOAEnabled=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8 -Ddruid.logType=slf4j"

if [ $JavaVersion -ge 11 ] ; then
  JAVA_OPTS=" $JAVA_OPTS -Xlog:gc*:$base_log/gc.log:time "
  JAVA_OPTS="$JAVA_OPTS"
else
  JAVA_OPTS=" $JAVA_OPTS -Xloggc:$base_log/gc.log -XX:+PrintGCDetails "
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime"
fi
JAVA_OPTS=" $JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$base_log  -XX:+CrashOnOutOfMemoryError -XX:ErrorFile=$base_log/hs_err_pid%p.log"
# JAVA_OPTS=" $JAVA_OPTS -XX:+UseWisp2"
TDDL_OPTS=" $TDDL_OPTS -Dlogback.configurationFile=$logback_configurationFile -Dtddl.conf=$tddl_conf"

if [ -e $tddl_conf -a -e $logback_configurationFile ]
then
	if [ x"$serverPort" != "x" ] && [ -d $base/$serverPort/lib ]; then
		#CLASSPATH=$(echo "$base"/$serverPort/lib/*.jar | tr ' ' ':'):"$CLASSPATH"
		for i in $base/$serverPort/lib/*; do
			CLASSPATH=$i:"$CLASSPATH";
		done
	else
		# Caution: put polardbx-calcite first to avoid conflict
		CALCITEPATH=$(echo "$base"/lib/*.jar | awk 'BEGIN{RS="[ \n]"} /polardbx-calcite/ {printf "%s:",$0}')
		OTHERPATH=$(echo "$base"/lib/*.jar | awk 'BEGIN{RS="[ \n]"} !/polardbx-calcite/ && !/^$/ {printf "%s:",$0}')
		CLASSPATH="$CALCITEPATH$OTHERPATH$CLASSPATH"
	fi

 	CLASSPATH="$base/conf:$CLASSPATH";

 	echo "cd to $bin_abs_path for workaround relative path"
  	cd $bin_abs_path

# For ECS and 2.6.32 kernel only
if [ "${idc%%_*}" == "ecs" ] && [ "$KERNEL_VERSION" == "2.6.32-220.23.2.al.ali1.1.alios6.x86_64" ]; then
   # Calculate the number of logical CPU cores and bind them to the DRDS process except for Core 0
   NUM_OF_CORES=`grep -i processor /proc/cpuinfo | wc -l`
   if [ $NUM_OF_CORES -gt 8 ]; then
      TASKSET="taskset -c 1-$((NUM_OF_CORES-1))"
   fi
fi

	echo LOG CONFIGURATION : $logback_configurationFile
	echo tddl conf : $tddl_conf
	echo CLASSPATH :$CLASSPATH
	echo JAVA_OPTS :$JAVA_OPTS
	echo TDDL_OPTS :$TDDL_OPTS
	if [[ $initializeGms == true || x"$engine" != "x" ]]; then
	  echo "initializing polardb-x"
    $TASKSET $JAVA $JAVA_OPTS $JAVA_DEBUG_OPT $TDDL_OPTS \
    -classpath .:$CLASSPATH com.alibaba.polardbx.server.TddlLauncher
  else
    echo "start polardb-x"
    $TASKSET $JAVA $BIANQUE_AGENT_OPTS $JAVA_OPTS $JAVA_DEBUG_OPT $TDDL_OPTS -classpath .:$CLASSPATH com.alibaba.polardbx.server.TddlLauncher --port=$serverPort 1>>$base_log/tddl-console.log 2>&1 &
    echo "$! #@# $args" > $pidfile
    echo "cd to $current_path for continue"
  	cd $current_path
  fi
else
	usage
fi
