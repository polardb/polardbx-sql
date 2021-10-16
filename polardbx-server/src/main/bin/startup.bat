@echo off
@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT"  setlocal

set ENV_PATH=.\
if "%OS%" == "Windows_NT" set ENV_PATH=%~dp0%

set conf_dir=%ENV_PATH%\..\conf
set tddl_conf=%conf_dir%\server.properties
set logback_configurationFile=%conf_dir%\logback.xml

set CLASSPATH=%conf_dir%
set CLASSPATH=%conf_dir%\..\lib\*;%CLASSPATH%

set JAVA_MEM_OPTS= -Xms128m -Xmx512m -XX:PermSize=96m
set JAVA_OPTS_EXT= -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dapplication.codeset=UTF-8 -Dfile.encoding=UTF-8
set JAVA_DEBUG_OPT= -server -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=9099,server=y,suspend=n
set TDDL_OPTS= -DappName=tddl -Dlogback.configurationFile="%logback_configurationFile%" -Dtddl.conf="%tddl_conf%"

set JAVA_OPTS= %JAVA_MEM_OPTS% %JAVA_OPTS_EXT% %JAVA_DEBUG_OPT% %TDDL_OPTS%

set CMD_STR= java %JAVA_OPTS% -classpath "%CLASSPATH%" java %JAVA_OPTS% -classpath "%CLASSPATH%" com.alibaba.cobar.CobarLauncher
echo start cmd : %CMD_STR%

java %JAVA_OPTS% -classpath "%CLASSPATH%" com.alibaba.polardbx.server.TddlLauncher