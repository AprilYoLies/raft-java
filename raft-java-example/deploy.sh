#!/usr/bin/env bash

mvn clean package

EXAMPLE_TAR=raft-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
NODE_DIR_PREFIX="node"
HOST="127.0.0.1"
HOST_LIST=""
PORT=8050
NODE_NUM=3

mkdir -p $ROOT_DIR
cd $ROOT_DIR || exit
rm -fr ./*

for ((i = 1; i <= NODE_NUM; i++)); do
  cur=$HOST
  ((port = PORT + i))
  cur=$cur:$port:$i
  HOST_LIST=$HOST_LIST$cur,
done
HOST_LIST=${HOST_LIST/%?/}
HOST_LIST=$HOST_LIST

for ((i = 1; i <= NODE_NUM; i++)); do
  cur_dir=$NODE_DIR_PREFIX$i
  mkdir $cur_dir
  cd $cur_dir || exit
  cp -f ../../target/$EXAMPLE_TAR .
  tar -zxf $EXAMPLE_TAR
  chmod +x ./bin/*.sh
  cur=$HOST
  ((port = PORT + i))
  cur=$cur:$port:$i
  cur=$cur
    nohup ./bin/run_server.sh ./data $HOST_LIST $cur &
  cd .. || exit
done
