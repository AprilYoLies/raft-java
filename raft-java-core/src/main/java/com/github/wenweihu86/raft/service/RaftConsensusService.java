package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftProto;

/**
 * raft节点之间相互通信的接口。
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusService {
    // 预投票
    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

    // 请求投票
    RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request);

    // 准备选举
    RaftProto.PrepareElectionResponse prepareElection(RaftProto.PrepareElectionRequest request);

    // 资格确认
    RaftProto.QualificationConfirmResponse qualificationConfirm(RaftProto.QualificationConfirmRequest request);

    // 资格写入
    RaftProto.QualificationWriteResponse qualificationWrite(RaftProto.QualificationWriteRequest request);

    // 优先级投票
    RaftProto.PriorityVoteResponse priorityVote(RaftProto.PriorityVoteRequest request);

    // 追加日志项
    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);

    // 安装快照
    RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
}
