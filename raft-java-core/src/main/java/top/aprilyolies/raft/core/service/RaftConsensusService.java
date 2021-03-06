package top.aprilyolies.raft.core.service;


import top.aprilyolies.raft.core.proto.RaftProto;

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

    // 并发追加日志项
    RaftProto.AppendEntriesResponse appendEntriesConcurrent(RaftProto.AppendEntriesRequest request);

    // 安装快照
    RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
}
