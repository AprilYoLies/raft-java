package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);
    private static final JsonFormat PRINTER = new JsonFormat(); // protobuf 和 json 打印相关的实例

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

    @Override   // 收到投票请求后作出的响应，就是比较谁的日志项更新，然后决定投票的结果
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();   // 构建 VoteResponse
            responseBuilder.setGranted(false);  // 默认是 false
            responseBuilder.setTerm(raftNode.getCurrentTerm()); // 自己的任期号
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build(); // 如果配置项中不包含请求投票的节点信息，直接否定
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build(); // 如果请求者的任期号不如自己的高，也需要否定
            }
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()  // 如果它的日志项任期比自己高
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()   // 或者咱俩的日志的任期一致，但是它的索引大于等于自己的
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk) {
                return responseBuilder.build(); // 如果它的索引不如自己的新，需要投否定票
            } else {
                responseBuilder.setGranted(true);   // 否则设置赞成标记
                responseBuilder.setTerm(raftNode.getCurrentTerm()); // 并将自己的任期号附带上
            }
            LOG.info("preVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());   // 日志记录收到了谁的投票请求，它的任期是多少，然后我投了赞成还是否定票
            return responseBuilder.build(); // 返回响应的结果
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override   // 节点收到投票请求后的操作，根据日志项的完整性及自己是否投了别人来决定自己的投票结果
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();   // 构建 VoteResponse
            responseBuilder.setGranted(false);  // 初始化为否定票
            responseBuilder.setTerm(raftNode.getCurrentTerm()); // 写入自己的任期号
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build(); // 如果配置项中不包含请求者的信息，直接投否定票
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build(); // 如果它的任期不如自己的高，直接投否定票
            }
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());   // 如果它的任期比自己的高，自己需要执行降级
            }
            boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()  // 如果它的日志任期比自己的日志任期高
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()   // 或者咱俩任期相同，但是它的索引值比自己的大
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (raftNode.getVotedFor() == 0 && logIsOk) {   // 如果还未给别人投过票，并且可以给它投票
                raftNode.stepDown(request.getTerm());   // 判断参数是否合理，然后重置一些状态信息，更新自己的身份为 Follower，终止心跳任务，重置选举定时器
                raftNode.setVotedFor(request.getServerId());    // 记录自己的投票对象
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null);  // 更新元数据信息
                responseBuilder.setGranted(true);   // 标记为赞成票
                responseBuilder.setTerm(raftNode.getCurrentTerm()); // 写入自己的任期号
            }
            LOG.info("RequestVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());   // 日志打印收到了谁的投票请求，任期号多少，我的任期号，我赞成或者否定票
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override   // 追加日志项，根据条件来决定是否完成追加，期间可能需要砍掉脏日志记录，也可能是收到的心跳消息，然后完成日志追加，日志项的应用，然后响应 Leader 结果
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.AppendEntriesResponse.Builder responseBuilder // 构建 AppendEntriesResponse
                    = RaftProto.AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(raftNode.getCurrentTerm()); // 设置自己的任期号
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);    // 默认的响应状态码
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());   // 自己最后一条日志项的索引
            if (request.getTerm() < raftNode.getCurrentTerm()) {    // 如果请求者的任期小于自己，否定追加操作
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());   // 判断参数是否合理，然后重置一些状态信息，更新自己的身份为 Follower，终止心跳任务，重置选举定时器
            if (raftNode.getLeaderId() == 0) {  // 如果自己还未记录过 Leader 节点的信息
                raftNode.setLeaderId(request.getServerId());    // 更新 Leader 节点的信息
                LOG.info("new leaderId={}, conf={}",    // 打印节点及 Leader 信息
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
            if (raftNode.getLeaderId() != request.getServerId()) {  // 如果自己当前记录的 Leader 信息和收到的日志追加请求的节点信息不一致
                LOG.warn("Another peer={} declares that it is the leader " +    // 打印警告信息
                                "at term={} which was occupied by leader={}",
                        request.getServerId(), request.getTerm(), raftNode.getLeaderId());
                raftNode.stepDown(request.getTerm() + 1);   // 这里为什么是它的任期加一呢？？
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {  // 如果自己没有上一条日志项
                LOG.info("Rejecting AppendEntries RPC would leave gap, " +
                        "request prevLogIndex={}, my lastLogIndex={}",
                        request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return responseBuilder.build(); // 响应追加失败，其实此时响应用已经有自己的最后一条日志项的索引值了
            }
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()   // 如果上一条日志项索引大于自己的第一条日志项
                    && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex())    // 且自己上一条日志项的任期和请求的不一致
                    != request.getPrevLogTerm()) {
                LOG.info("Rejecting AppendEntries RPC: terms don't agree, " +   // 上一条日志项不匹配
                        "request prevLogTerm={} in prevLogIndex={}, my is {}",
                        request.getPrevLogTerm(), request.getPrevLogIndex(),
                        raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                Validate.isTrue(request.getPrevLogIndex() > 0); // 上一条日志项的索引大于 0
                responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1); // 告诉 Leader 自己需要的日志项是上一条日志项
                return responseBuilder.build();
            }

            if (request.getEntriesCount() == 0) {   // 如果追加的日志项长度为 0
                LOG.debug("heartbeat request from peer={} at term={}, my term={}",  // 此时收到的就是心跳消息
                        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 响应心跳收到
                responseBuilder.setTerm(raftNode.getCurrentTerm()); // 告诉 Leader 自己的任期号
                responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());   // 告诉 Leader 自己最后一条日志项的索引
                advanceCommitIndex(request);    // 确定新的 commitIndex，然后让状态机应用日志项直到新的 commitIndex
                return responseBuilder.build(); // 向 Leader 节点返回心跳消息
            }

            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 执行到这里，说明不是心跳消息
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex(); // 上一条日志项的索引
            for (RaftProto.LogEntry entry : request.getEntriesList()) {
                index++;
                if (index < raftNode.getRaftLog().getFirstLogIndex()) { // 跳过自己第一条日志项之前的全部日志项
                    continue;
                }
                if (raftNode.getRaftLog().getLastLogIndex() >= index) { // 如果自己最后一条日志项大于当前的日志项
                    if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) { // 如果自己已经存在的日志项跟收到的日志项任期一致
                        continue;   // 继续
                    }
                    // truncate segment log from index
                    long lastIndexKept = index - 1;
                    raftNode.getRaftLog().truncateSuffix(lastIndexKept);    // 砍掉之后不匹配的日志项
                }   // 删除 newEndIndex 之后的日志项，包括 Segment 的 Record list 和日志段文件中内容，更新 Segment 相关的信息，重命名文件
                entries.add(entry); // 将日志项添加到 list 中
            }
            raftNode.getRaftLog().append(entries);  // 将收到的日志项追加到本地中
            raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), // 更新元数据信息
                    null, raftNode.getRaftLog().getFirstLogIndex());
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());   // 响应中告诉 Leader 自己接下来需要的日志项的位置

            advanceCommitIndex(request);    // 确定新的 commitIndex，然后让状态机应用日志项直到新的 commitIndex
            LOG.info("AppendEntries request from server {} " +  // 收到了谁的日志追加请求，任期是多少，我的任期是多少，日志项条数，自己的追加结果如何
                            "in term {} (my term is {}), entryCount={} resCode={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),
                    request.getEntriesCount(), responseBuilder.getResCode());
            return responseBuilder.build(); // 返回给 Leader 节点日志追加的结果
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder
                = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);    // 默认初始化为失败

        raftNode.getLock().lock();
        try {
            responseBuilder.setTerm(raftNode.getCurrentTerm()); // 填充自己的任期号
            if (request.getTerm() < raftNode.getCurrentTerm()) {    // 如果它的任期比自己低
                return responseBuilder.build(); // 拒绝快照安装请求
            }
            raftNode.stepDown(request.getTerm());   // 确保自己是 follower 身份，同时重置选举定时器
            if (raftNode.getLeaderId() == 0) {  // 如果自己未记录过 Leader id
                raftNode.setLeaderId(request.getServerId());    // 记录 Leader id
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (raftNode.getSnapshot().getIsTakeSnapshot().get()) { // 如果自己正在拍快照
            LOG.warn("alreay in take snapshot, do not handle install snapshot request now");
            return responseBuilder.build(); // 拒绝安装快照的请求
        }

        raftNode.getSnapshot().getIsInstallSnapshot().set(true);    // 设置自己正在安装快照的状态
        RandomAccessFile randomAccessFile = null;
        raftNode.getSnapshot().getLock().lock();
        try {
            // write snapshot data to local
            String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";   // 临时快照目录
            File file = new File(tmpSnapshotDir);   // 对应临时快照目录的 File
            if (request.getIsFirst()) { // 如果本次请求是第一个文件批次
                if (file.exists()) {    // 删除已经存在的快照目录
                    file.delete();
                }
                file.mkdir();   // 重新创建快照文件目录
                LOG.info("begin accept install snapshot request from serverId={}", request.getServerId());
                raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,   // 更新自己的快照元信息
                        request.getSnapshotMetaData().getLastIncludedIndex(),   // 包括最后一个日志项的索引
                        request.getSnapshotMetaData().getLastIncludedTerm(),    // 最后一个日志项的任期
                        request.getSnapshotMetaData().getConfiguration());  // 快照对应的配置信息
            }
            // write to file
            String currentDataDirName = tmpSnapshotDir + File.separator + "data";   // 当前的快照文件目录
            File currentDataDir = new File(currentDataDirName); // 代表当前快照文件目录的 File
            if (!currentDataDir.exists()) { // 如果这个文件夹已经存在
                currentDataDir.mkdirs();    // 删除
            }

            String currentDataFileName = currentDataDirName + File.separator + request.getFileName();   // 收到的快照文件的名字
            File currentDataFile = new File(currentDataFileName);   // 对应收到的快照文件的 File
            // 文件名可能是个相对路径，比如topic/0/message.txt
            if (!currentDataFile.getParentFile().exists()) {    // 如果保存文件的上级目录不存在
                currentDataFile.getParentFile().mkdirs();   // 创建上级目录
            }
            if (!currentDataFile.exists()) {    // 如果快照文件不存在
                currentDataFile.createNewFile();    // 创建快照文件
            }
            randomAccessFile = RaftFileUtils.openFile(  // 打开对应的快照文件
                    tmpSnapshotDir + File.separator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.seek(request.getOffset()); // 定位写入位置
            randomAccessFile.write(request.getData().toByteArray());    // 将收到的数据写入快照文件
            // move tmp dir to snapshot dir if this is the last package
            if (request.getIsLast()) {  // 如果当前收到的是最后一个文件批次，需要将收到的全部快照文件移动了真正的快照文件目录
                File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());   // 真正的快照文件夹
                if (snapshotDirFile.exists()) { // 删除存在的快照文件夹
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile); // 将收到的临时快照文件夹中的快照文件移动到真正的快照文件夹中
            }
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 响应 Leader 本批次的快照文件接收成功
            LOG.info("install snapshot request from server {} " +
                            "in term {} (my term is {}), resCode={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getResCode());
        } catch (IOException ex) {
            LOG.warn("when handle installSnapshot request, meet exception:", ex);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);  // 关闭正在写入的快照文件
            raftNode.getSnapshot().getLock().unlock();
        }
        // 如果当前收到的是最后一个批次的快照文件并且数据写入成功了
        if (request.getIsLast() && responseBuilder.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            // apply state machine
            // TODO: make this async
            String snapshotDataDir = raftNode.getSnapshot().getSnapshotDir() + File.separator + "data"; // 真正的快照文件目录
            raftNode.getStateMachine().readSnapshot(snapshotDataDir);   // 交由状态机应用快照，就是将快照文件移动到 RocksDB 对应的数据目录，然后让 RocksDB 打开快照即可
            long lastSnapshotIndex;
            // 重新加载snapshot
            raftNode.getSnapshot().getLock().lock();
            try {
                raftNode.getSnapshot().reload();    // 尝试从本地获取快照元数据，没有的话就构建一个
                lastSnapshotIndex = raftNode.getSnapshot().getMetaData().getLastIncludedIndex();    // 元数据中记录的最后一个日志项的索引
            } finally {
                raftNode.getSnapshot().getLock().unlock();
            }

            // discard old log entries
            raftNode.getLock().lock();
            try {
                raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);    // 删除 lastSnapshotIndex + 1 之前的全部日志项相关的内容
            } finally {
                raftNode.getLock().unlock();
            }
            LOG.info("end accept install snapshot request from serverId={}", request.getServerId());
        }

        if (request.getIsLast()) {
            raftNode.getSnapshot().getIsInstallSnapshot().set(false);   // 如果快照安装完成，修改最后的状态
        }

        return responseBuilder.build();
    }

    // in lock, for follower 确定新的 commitIndex，然后让状态机应用日志项直到新的 commitIndex
    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
        long newCommitIndex = Math.min(request.getCommitIndex(),    // 新的 commitIndex 不能超过 Leader 的 commitIndex
                request.getPrevLogIndex() + request.getEntriesCount());
        raftNode.setCommitIndex(newCommitIndex);    // 更新自己的 commitIndex
        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {   // 如果自己的 applyIndex 小于 commitIndex
            // apply state machine
            for (long index = raftNode.getLastAppliedIndex() + 1;
                 index <= raftNode.getCommitIndex(); index++) { // 从第一条没被 apply 的日志项开始往后执行
                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);   // 拿到对应的日志项
                if (entry != null) {
                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
                        raftNode.getStateMachine().apply(entry.getData().toByteArray());    // 如果是数据，交由状态机执行
                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        raftNode.applyConfiguration(entry); // 如果是配置项，直接应用
                    }
                }
                raftNode.setLastAppliedIndex(index);
            }
        }
    }

}
