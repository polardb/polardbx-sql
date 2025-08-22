#!/bin/sh

baseDir=`pwd`/
target_branch="$1"
commit=`git merge-base origin/$target_branch HEAD`
files=`git diff --name-only ${commit} | grep '.java' | grep -v 'polardbx-calcite' | grep -v 'polardbx-orc' | grep -v 'polardbx-rpc/src/main/java/com/mysql/cj'| grep -v 'com/alibaba/polardbx/rpc/cdc'| xargs -I {} echo ${baseDir}{}`
count=0
batchFile=''
for file in $files; do
  if [ -f "$file" ]; then
	  count=$((($count + 1) % 100))
	  batchFile=$batchFile' '$file
	  if [[ $count -eq 0 ]]; then
		  /home/admin/idea/bin/format.sh -s ${baseDir}'codestyle/codestyle-idea.xml' -m '*.java' ${batchFile}
		  batchFile=''
	  fi
	  trap "echo Exited!; exit;" SIGINT SIGTERM
	fi
done

if [[ ! -z $batchFile ]]; then
	/home/admin/idea/bin/format.sh -s ${baseDir}'codestyle/codestyle-idea.xml' -m '*.java' ${batchFile}
fi
