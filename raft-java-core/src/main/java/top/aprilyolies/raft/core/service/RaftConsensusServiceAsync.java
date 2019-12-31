package top.aprilyolies.raft.core.service;


import com.baidu.brpc.client.RpcCallback;
import top.aprilyolies.raft.core.proto.RaftProto;

import java.util.concurrent.Future;

/**
 * 用于生成client异步调用所需的proxy
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusServiceAsync extends RaftConsensusService {
    // 预投票
    Future<RaftProto.VoteResponse> preVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);

    // 准备选举
    Future<RaftProto.PrepareElectionResponse> prepareElection(
            RaftProto.PrepareElectionRequest request,
            RpcCallback<RaftProto.PrepareElectionResponse> callback);

    // 资格确认
    Future<RaftProto.QualificationConfirmResponse> qualificationConfirm(
            RaftProto.QualificationConfirmRequest request,
            RpcCallback<RaftProto.QualificationConfirmResponse> callback);

    // 资格写入
    Future<RaftProto.QualificationWriteResponse> qualificationWrite(
            RaftProto.QualificationWriteRequest request,
            RpcCallback<RaftProto.QualificationWriteResponse> callback);

    // 资格写入
    Future<RaftProto.PriorityVoteResponse> priorityVote(
            RaftProto.PriorityVoteRequest request,
            RpcCallback<RaftProto.PriorityVoteResponse> callback);

    // 请求投票
    Future<RaftProto.VoteResponse> requestVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);

    // 追加日志项
    Future<RaftProto.AppendEntriesResponse> appendEntries(
            RaftProto.AppendEntriesRequest request,
            RpcCallback<RaftProto.AppendEntriesResponse> callback);

    // 安装快照
    Future<RaftProto.InstallSnapshotResponse> installSnapshot(
            RaftProto.InstallSnapshotRequest request,
            RpcCallback<RaftProto.InstallSnapshotResponse> callback);
}
