#!/usr/bin/env bash

mvn clean package

EXAMPLE_TAR=raft-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
NODE_DIR_PREFIX="node"
HOST="127.0.0.1"
HOST_LIST=""
PORT=8050
NODE_NUM=7
ELECTION_TIMES=200

mkdir -p $ROOT_DIR
cd $ROOT_DIR || exit
rm -fr ./*

# 复制 tar 包并解压
for ((i = 1; i <= NODE_NUM; i++)); do
  cur_dir=$NODE_DIR_PREFIX$i
  mkdir $cur_dir
  cd $cur_dir || exit
  cp -f ../../target/$EXAMPLE_TAR .
  tar -zxf $EXAMPLE_TAR
  chmod +x ./bin/*.sh
  cd .. || exit
done

# 拼接 Server 节点信息
for ((i = 1; i <= NODE_NUM; i++)); do
  cur=$HOST
  ((port = PORT + i))
  cur=$cur:$port:$i
  HOST_LIST=$HOST_LIST$cur,
done
HOST_LIST=${HOST_LIST/%?/}
HOST_LIST=$HOST_LIST

# 测试 Leader 选举用时
for ((j = 1; j <= ELECTION_TIMES; j++)); do
  for ((i = 1; i <= NODE_NUM; i++)); do
    cur_dir=$NODE_DIR_PREFIX$i
    cd $cur_dir || exit
    rm -fr ./data
    cd .. || exit
  done
  for ((i = 1; i <= NODE_NUM; i++)); do
    cur_dir=$NODE_DIR_PREFIX$i
    cd $cur_dir || exit
    cur=$HOST
    ((port = PORT + i))
    cur=$cur:$port:$i
    cur=$cur
    nohup ./bin/run_server.sh ./data $HOST_LIST $cur &
    cd .. || exit
  done
  sleep 12
  jps | grep ServerMain | cut -c 1-5 | xargs kill -9
  sleep 3
done
