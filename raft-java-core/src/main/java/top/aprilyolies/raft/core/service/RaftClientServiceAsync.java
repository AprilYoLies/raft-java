package top.aprilyolies.raft.core.service;

import com.baidu.brpc.client.RpcCallback;
import top.aprilyolies.raft.core.proto.RaftProto;

import java.util.concurrent.Future;

public interface RaftClientServiceAsync extends RaftClientService {

    Future<RaftProto.GetLeaderResponse> getLeader(
            RaftProto.GetLeaderRequest request,
            RpcCallback<RaftProto.GetLeaderResponse> callback);

    Future<RaftProto.GetConfigurationResponse> getConfiguration(
            RaftProto.GetConfigurationRequest request,
            RpcCallback<RaftProto.GetConfigurationResponse> callback);

    Future<RaftProto.AddPeersResponse> addPeers(
            RaftProto.AddPeersRequest request,
            RpcCallback<RaftProto.AddPeersResponse> callback);

    Future<RaftProto.RemovePeersResponse> removePeers(
            RaftProto.RemovePeersRequest request,
            RpcCallback<RaftProto.RemovePeersResponse> callback);
}
