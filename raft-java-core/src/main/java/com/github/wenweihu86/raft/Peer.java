package com.github.wenweihu86.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftConsensusServiceAsync;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {
    private RaftProto.Server server;
    private RpcClient rpcClient;
    private RaftConsensusServiceAsync raftConsensusServiceAsync;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;
    // 保存了 Server 实例，构建了 RpcClient
    public Peer(RaftProto.Server server) {
        this.server = server;   // 两个字段 server_id、endpoints
        this.rpcClient = new RpcClient(new Endpoint(    // rpc 客户端
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()));   // 通过 RpcClient 构建 RaftConsensusServiceAsync 的代理类
        raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class); // 对应的服务代理
        isCatchUp = false;
    }

    public RaftProto.Server getServer() {
        return server;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }
    // 获取用于 rpc 通信的 RaftConsensusServiceAsync 代理类
    public RaftConsensusServiceAsync getRaftConsensusServiceAsync() {
        return raftConsensusServiceAsync;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
}
