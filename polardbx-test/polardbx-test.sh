#!/usr/bin/env bash
echo "第一个参数为：$1"
IFS=$','
baseClassPath="com/alibaba/polardbx/qatest/"
testClasses=""
for i in $(echo "${1}")
do
    if [ "$testClasses" =  ""  ]; then
        testClasses=${baseClassPath}${i}"/**"
    else
        testClasses=${testClasses},${baseClassPath}${i}"/**"
    fi
done
echo "mvn test  -Dmaven.test.failure.ignore=true -Dtest="${testClasses}" -Ddeploy.skip=true -Djdk=1.8"
mvn test  -Dmaven.test.failure.ignore=true -Dtest="${testClasses}" -Ddeploy.skip=true -Djdk=1.8



