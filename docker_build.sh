#!/bin/bash

current_path=`pwd`
case "`uname`" in
    Darwin)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
    Linux)
        bin_abs_path=$(readlink -f $(dirname $0))
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac
BASE=${bin_abs_path}

cd $BASE/ && mvn -Dmaven.test.skip=true clean package install
docker build -f docker/Dockerfile -t polardbx/polardbx-cn $BASE/
