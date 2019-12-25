package com.github.wenweihu86.raft.example.server;

import com.baidu.brpc.server.RpcServer;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.example.server.service.impl.ExampleServiceImpl;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.service.impl.RaftClientServiceImpl;
import com.github.wenweihu86.raft.service.impl.RaftConsensusServiceImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ServerMain {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.printf("Usage: ./run_server.sh DATA_PATH CLUSTER CURRENT_NODE\n");
            System.exit(-1);
        }
        // parse args
        // raft data dir
        String dataPath = args[0];
        // peers, format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[1];
        String[] splitArray = servers.split(",");
        List<RaftProto.Server> serverList = new ArrayList<>();
        for (String serverString : splitArray) {
            RaftProto.Server server = parseServer(serverString);    // 应该就是通过服务器的地址及端口信息构建了 RaftProto.Server 实例
            serverList.add(server);
        }
        // local server
        RaftProto.Server localServer = parseServer(args[2]);    // 应该就是通过服务器的地址及端口信息构建了 RaftProto.Server 实例

        // 初始化RPCServer
        RpcServer server = new RpcServer(localServer.getEndpoint().getPort());  // 真正用于通信的 RPC server
        // 设置Raft选项，比如：
        // just for test snapshot
        RaftOptions raftOptions = new RaftOptions();    // 和 raft 相关的属性
        raftOptions.setDataDir(dataPath);
        raftOptions.setSnapshotMinLogSize(10 * 1024);   // 最小快照长度 10 KB
        raftOptions.setSnapshotPeriodSeconds(30);   // 快照时间间隔 30 S
        raftOptions.setMaxSegmentFileSize(1024 * 1024); // 最大日志段文件长度 1 MB
        // 应用状态机
        ExampleStateMachine stateMachine = new ExampleStateMachine(raftOptions.getDataDir());   // 创建 ExampleStateMachine，保存了路径
        // 初始化RaftNode，保存了 raftOptions，构建了 RaftProto.Configuration，创建 snapshot 并尝试从本地加载快照元数据，创建 raftLog 并加载了本地元数据，比较快照范围，执行后续的日志项，更新 applyIndex
        RaftNode raftNode = new RaftNode(raftOptions, serverList, localServer, stateMachine);
        // 注册Raft节点之间相互调用的服务
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode); // 将 RaftNode 保存到 RaftConsensusServiceImpl 实例中
        server.registerService(raftConsensusService);   // 将当前的服务注册到 RpcServer
        // 注册给Client调用的Raft服务
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);  // 将 RaftNode 保存到 RaftClientServiceImpl 实例中
        server.registerService(raftClientService);  // 将当前的服务注册到 RpcServer
        // 注册应用自己提供的服务
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine); // 将 RaftNode,ExampleStateMachine 保存到 ExampleServiceImpl 实例中
        server.registerService(exampleService); // 将当前的服务注册到 RpcServer
        // 启动RPCServer，初始化Raft节点
        server.start(); // 仅仅是启动 RpcServer
        raftNode.init();    // 将其它节点封装为 Peer，里边保存了 RpcClient 以及下一个日志项索引
    }
    // 应该就是通过服务器的地址及端口信息构建了 RaftProto.Server 实例
    private static RaftProto.Server parseServer(String serverString) {
        String[] splitServer = serverString.split(":");
        String host = splitServer[0];
        Integer port = Integer.parseInt(splitServer[1]);
        Integer serverId = Integer.parseInt(splitServer[2]);
        RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                .setHost(host).setPort(port).build();
        RaftProto.Server.Builder serverBuilder = RaftProto.Server.newBuilder();
        RaftProto.Server server = serverBuilder.setServerId(serverId).setEndpoint(endPoint).build();
        return server;
    }
}
