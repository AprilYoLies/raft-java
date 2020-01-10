#!/usr/bin/env bash

# 需要修改 ServerMain 的
mvn clean package

EXAMPLE_TAR=raft-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
NODE_DIR_PREFIX="node"
HOST="127.0.0.1"
HOST_LIST=""
PORT=8050
NODE_NUM=3
ELECTION_TIMES=200

mkdir -p $ROOT_DIR
cd $ROOT_DIR || exit
rm -fr ./*

# 3 7 11 15 个节点
for ((k = 0; k < 4; k++)); do
  # 拼接 Server 节点信息
  ((CUR_NODE_NUM = NODE_NUM + k * 4))

  echo "processing node num "$CUR_NODE_NUM
  # 复制 tar 包并解压
  for ((i = 1; i <= CUR_NODE_NUM; i++)); do
    cur_dir=$NODE_DIR_PREFIX$i
    mkdir $cur_dir
    cd $cur_dir || exit
    cp -f ../../target/$EXAMPLE_TAR .
    tar -zxf $EXAMPLE_TAR
    chmod +x ./bin/*.sh
    cd .. || exit
  done

  CUR_HOST_LIST=$HOST_LIST
  for ((i = 1; i <= CUR_NODE_NUM; i++)); do
    cur=$HOST
    ((port = PORT + i))
    cur=$cur:$port:$i
    CUR_HOST_LIST=$CUR_HOST_LIST$cur,
  done
  CUR_HOST_LIST=${CUR_HOST_LIST/%?/}

  # 测试 Leader 选举用时
  for ((j = 1; j <= ELECTION_TIMES; j++)); do # 选举 ELECTION_TIMES 次
    for ((i = 1; i <= CUR_NODE_NUM; i++)); do # 清理元数据信息
      cur_dir=$NODE_DIR_PREFIX$i
      cd $cur_dir || exit
      rm -fr ./data
      cd .. || exit
    done
    for ((i = 1; i <= CUR_NODE_NUM; i++)); do # 启动 CUR_NODE_NUM 个节点
      cur_dir=$NODE_DIR_PREFIX$i
      cd $cur_dir || exit
      cur=$HOST
      ((port = PORT + i))
      cur=$cur:$port:$i
      cur=$cur
      nohup ./bin/run_server.sh ./data $CUR_HOST_LIST $cur &
      cd .. || exit
    done
    sleep 10
    jps | grep ServerMain | cut -f 1 -d " " | xargs kill -9
    sleep 2
  done
  dir_name="node_num_"$CUR_NODE_NUM
  cp -r ../env ../$dir_name
  rm -fr ./*
  find ../$dir_name -name raft-java-example-1.9.0-deploy.tar.gz | xargs rm -fr
  find ../$dir_name -name bin | xargs rm -fr
  find ../$dir_name -name data | xargs rm -fr
  find ../$dir_name -name lib | xargs rm -fr
  find ../$dir_name -name logs | xargs rm -fr
done
