package com.github.wenweihu86.raft;

import com.baidu.brpc.client.RpcCallback;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.storage.SegmentedLog;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.google.protobuf.ByteString;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Created by wenweihu86 on 2017/5/2.
 * 该类是raft核心类，主要有如下功能：
 * 1、保存raft节点核心数据（节点状态信息、日志信息、snapshot等），
 * 2、raft节点向别的raft发起rpc请求相关函数
 * 3、raft节点定时器：主节点心跳定时器、发起选举定时器。
 */
public class RaftNode {

    public enum NodeState {
        STATE_FOLLOWER,
        STATE_PRE_CANDIDATE,
        STATE_CANDIDATE,
        STATE_LEADER
    }

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);
    private static final JsonFormat jsonFormat = new JsonFormat();  // 应该是 protobuf 和 json 相关的字段

    private RaftOptions raftOptions;
    private RaftProto.Configuration configuration;
    private ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<>();
    private RaftProto.Server localServer;
    private StateMachine stateMachine;
    private SegmentedLog raftLog;
    private Snapshot snapshot;

    private NodeState state = NodeState.STATE_FOLLOWER; // 初始状态为 STATE_FOLLOWER
    // 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private long currentTerm;
    // 在当前获得选票的候选人的Id
    private int votedFor;
    private int leaderId; // leader节点id
    // 已知的最大的已经被提交的日志条目的索引值
    private long commitIndex;
    // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private volatile long lastAppliedIndex;

    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = lock.newCondition();   // 两个 condition 量
    private Condition catchUpCondition = lock.newCondition();

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;
    // 保存了 raftOptions，构建了 RaftProto.Configuration，创建 snapshot 并尝试从本地加载快照元数据，创建 raftLog 并加载了本地元数据，比较快照范围，执行后续的日志项，更新 applyIndex
    public RaftNode(RaftOptions raftOptions,
                    List<RaftProto.Server> servers,
                    RaftProto.Server localServer,
                    StateMachine stateMachine) {
        this.raftOptions = raftOptions;
        RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder(); // 协议的 Configuration，protobuf 生成
        for (RaftProto.Server server : servers) {
            confBuilder.addServers(server);
        }
        configuration = confBuilder.build();

        this.localServer = localServer;
        this.stateMachine = stateMachine;

        // load log and snapshot
        raftLog = new SegmentedLog(raftOptions.getDataDir(), raftOptions.getMaxSegmentFileSize());  // 尝试加载段数据（本地），加载了本地的元数据
        snapshot = new Snapshot(raftOptions.getDataDir());  // 创建快照类，主要是创建了快照对应的目录
        snapshot.reload();  // 尝试从本地获取快照元数据，没有的话就构建一个

        currentTerm = raftLog.getMetaData().getCurrentTerm();   // 获取日志元数据中的任期号
        votedFor = raftLog.getMetaData().getVotedFor(); // 获取日志元数据中的投票值
        commitIndex = Math.max(snapshot.getMetaData().getLastIncludedIndex(), commitIndex); // 确定 commit index
        // discard old log entries 丢弃过期日志项
        if (snapshot.getMetaData().getLastIncludedIndex() > 0
                && raftLog.getFirstLogIndex() <= snapshot.getMetaData().getLastIncludedIndex()) {
            raftLog.truncatePrefix(snapshot.getMetaData().getLastIncludedIndex() + 1);  // 进行快照后的日志项需要删除
        }
        // apply state machine
        RaftProto.Configuration snapshotConfiguration = snapshot.getMetaData().getConfiguration();
        if (snapshotConfiguration.getServersCount() > 0) {
            configuration = snapshotConfiguration;
        }
        String snapshotDataDir = snapshot.getSnapshotDir() + File.separator + "data";   // 快照目录
        stateMachine.readSnapshot(snapshotDataDir); // 通过状态机读取快照，如果 rocksdb 文件夹存在，删除，然后将快照的文件夹拷贝到 rocksdb 中，最后构建 RocksDB，打开位置为 rocksdb 目录
        for (long index = snapshot.getMetaData().getLastIncludedIndex() + 1;
             index <= commitIndex; index++) {   // 如果快照之后到 commitIndex 之间有剩余的日志项
            RaftProto.LogEntry entry = raftLog.getEntry(index); // 获取这条日志项
            if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {   // 如果这个日志项是数据，那么就交由状态机执行
                stateMachine.apply(entry.getData().toByteArray());
            } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {   // 如果是配置项块，应用配置项
                applyConfiguration(entry);
            }
        }
        lastAppliedIndex = commitIndex; // 更新 applyIndex 到 commitIndex
    }

    public void init() {    // 将其它节点封装为 Peer，里边保存了 RpcClient 以及下一个日志项索引
        for (RaftProto.Server server : configuration.getServersList()) {
            if (!peerMap.containsKey(server.getServerId())
                    && server.getServerId() != localServer.getServerId()) { // 这里应该是将其它节点封装为 Peer
                Peer peer = new Peer(server);   // 保存了 Server 实例，构建了 RpcClient，返回 Peer
                peer.setNextIndex(raftLog.getLastLogIndex() + 1);   // 维护的其它节点的日志位置，初始为 LastLogIndex + 1
                peerMap.put(server.getServerId(), peer);    // 将其它节点的信息保存到 peerMap 中
            }
        }

        // init thread pool
        executorService = new ThreadPoolExecutor(
                raftOptions.getRaftConsensusThreadNum(),
                raftOptions.getRaftConsensusThreadNum(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                takeSnapshot();
            }
        }, raftOptions.getSnapshotPeriodSeconds(), raftOptions.getSnapshotPeriodSeconds(), TimeUnit.SECONDS);
        // start election
        resetElectionTimer();
    }

    // client set command
    public boolean replicate(byte[] data, RaftProto.EntryType entryType) {
        lock.lock();
        long newLastLogIndex = 0;
        try {
            if (state != NodeState.STATE_LEADER) {
                LOG.debug("I'm not the leader");
                return false;
            }
            RaftProto.LogEntry logEntry = RaftProto.LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setType(entryType)
                    .setData(ByteString.copyFrom(data)).build();
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            entries.add(logEntry);
            newLastLogIndex = raftLog.append(entries);
            raftLog.updateMetaData(currentTerm, null, raftLog.getFirstLogIndex());

            for (RaftProto.Server server : configuration.getServersList()) {
                final Peer peer = peerMap.get(server.getServerId());
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        appendEntries(peer);
                    }
                });
            }

            if (raftOptions.isAsyncWrite()) {
                // 主节点写成功后，就返回。
                return true;
            }

            // sync wait commitIndex >= newLastLogIndex
            long startTime = System.currentTimeMillis();
            while (lastAppliedIndex < newLastLogIndex) {
                if (System.currentTimeMillis() - startTime >= raftOptions.getMaxAwaitTimeout()) {
                    break;
                }
                commitIndexCondition.await(raftOptions.getMaxAwaitTimeout(), TimeUnit.MILLISECONDS);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            lock.unlock();
        }
        LOG.debug("lastAppliedIndex={} newLastLogIndex={}", lastAppliedIndex, newLastLogIndex);
        if (lastAppliedIndex < newLastLogIndex) {
            return false;
        }
        return true;
    }

    public void appendEntries(Peer peer) {
        RaftProto.AppendEntriesRequest.Builder requestBuilder = RaftProto.AppendEntriesRequest.newBuilder();    // 构建一个追加日志的消息 Builder
        long prevLogIndex;
        long numEntries;

        boolean isNeedInstallSnapshot = false;
        lock.lock();
        try {
            long firstLogIndex = raftLog.getFirstLogIndex();    // 获取第一个日志项的索引值
            if (peer.getNextIndex() < firstLogIndex) {  // 判断是否需要安装快照（Peer 下一日志项在本机日志项索引之前，需要安装快照）
                isNeedInstallSnapshot = true;
            }
        } finally {
            lock.unlock();
        }

        LOG.debug("is need snapshot={}, peer={}", isNeedInstallSnapshot, peer.getServer().getServerId());
        if (isNeedInstallSnapshot) {
            if (!installSnapshot(peer)) {   // 如果需要安装快照，进行快照的安装操作
                return;
            }
        }

        long lastSnapshotIndex;
        long lastSnapshotTerm;
        snapshot.getLock().lock();  // 获取快照读取的锁
        try {
            lastSnapshotIndex = snapshot.getMetaData().getLastIncludedIndex();  // 快照元信息中最后一个日志项的索引
            lastSnapshotTerm = snapshot.getMetaData().getLastIncludedTerm();    // 快照元信息中最后一个日志项的任期
        } finally {
            snapshot.getLock().unlock();
        }

        lock.lock();
        try {
            long firstLogIndex = raftLog.getFirstLogIndex();    // 第一条日志项
            Validate.isTrue(peer.getNextIndex() >= firstLogIndex);  // 这时一定要求 Peer 的下一个位置是在本机日志项之后的
            prevLogIndex = peer.getNextIndex() - 1; // 目标日志项的上一条日志项
            long prevLogTerm;
            if (prevLogIndex == 0) {    // 如果之前没有日志项，假定之前的任期为 0
                prevLogTerm = 0;
            } else if (prevLogIndex == lastSnapshotIndex) { // 这里是以最后一个快照的最后一个日志项作为上一个日志项的索引值和任期
                prevLogTerm = lastSnapshotTerm;
            } else {
                prevLogTerm = raftLog.getEntryTerm(prevLogIndex);   // 直接获取索引位置对应的日志项
            }
            requestBuilder.setServerId(localServer.getServerId());  // 本机 id
            requestBuilder.setTerm(currentTerm);    // 当前任期
            requestBuilder.setPrevLogTerm(prevLogTerm); // 上一个日志项任期
            requestBuilder.setPrevLogIndex(prevLogIndex);   // 上一个日志项的索引
            numEntries = packEntries(peer.getNextIndex(), requestBuilder);  // 将多条日志项打包到 AppendEntriesRequest 中，返回打包的数量
            requestBuilder.setCommitIndex(Math.min(commitIndex, prevLogIndex + numEntries));    // commitIndex 不能超过自身的 commitIndex
        } finally {
            lock.unlock();
        }

        RaftProto.AppendEntriesRequest request = requestBuilder.build();    // 构建真正的 AppendEntriesRequest
        RaftProto.AppendEntriesResponse response = peer.getRaftConsensusServiceAsync().appendEntries(request);  // 发送追加日志项的请求

        lock.lock();
        try {
            if (response == null) { // 如果响应为 null，那么可以确定向 peer 追加日志时失败了
                LOG.warn("appendEntries with peer[{}:{}] failed",
                        peer.getServer().getEndpoint().getHost(),
                        peer.getServer().getEndpoint().getPort());  // 如果配置列表中此时不包含该 peer 了，需要将该 peer 移除并关闭其持有的 rpc 客户端
                if (!ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
                    peerMap.remove(peer.getServer().getServerId());
                    peer.getRpcClient().stop();
                }
                return;
            }
            LOG.info("AppendEntries response[{}] from server {} " + // 日志追加成功
                            "in term {} (my term is {})",
                    response.getResCode(), peer.getServer().getServerId(),
                    response.getTerm(), currentTerm);

            if (response.getTerm() > currentTerm) { // 如果被追加日志项的节点的任期比自己的高，需要降级
                stepDown(response.getTerm());
            } else {
                if (response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {  // 如果响应结果代码为成功
                    peer.setMatchIndex(prevLogIndex + numEntries);  // 更新 peer 的 matchIndex
                    peer.setNextIndex(peer.getMatchIndex() + 1);    // 更新 peer 的 nextIndex
                    if (ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
                        advanceCommitIndex();   // 根据各个节点的 matchIndex 来计算得到 commitIndex（超过半数的匹配即可提交），本机执行日志项到 commitIndex，更新 applyIndex
                    } else {
                        if (raftLog.getLastLogIndex() - peer.getMatchIndex() <= raftOptions.getCatchupMargin()) {
                            LOG.debug("peer catch up the leader");
                            peer.setCatchUp(true);
                            // signal the caller thread
                            catchUpCondition.signalAll();
                        }
                    }
                } else {
                    peer.setNextIndex(response.getLastLogIndex() + 1);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // in lock
    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            LOG.error("can't be happened");
            return;
        }
        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = 0;
            votedFor = 0;
            raftLog.updateMetaData(currentTerm, votedFor, null);
        }
        state = NodeState.STATE_FOLLOWER;
        // stop heartbeat
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        resetElectionTimer();
    }

    public void takeSnapshot() {
        if (snapshot.getIsInstallSnapshot().get()) {
            LOG.info("already in install snapshot, ignore take snapshot");
            return;
        }

        snapshot.getIsTakeSnapshot().compareAndSet(false, true);
        try {
            long localLastAppliedIndex;
            long lastAppliedTerm = 0;
            RaftProto.Configuration.Builder localConfiguration = RaftProto.Configuration.newBuilder();
            lock.lock();
            try {
                if (raftLog.getTotalSize() < raftOptions.getSnapshotMinLogSize()) {
                    return;
                }
                if (lastAppliedIndex <= snapshot.getMetaData().getLastIncludedIndex()) {
                    return;
                }
                localLastAppliedIndex = lastAppliedIndex;
                if (lastAppliedIndex >= raftLog.getFirstLogIndex()
                        && lastAppliedIndex <= raftLog.getLastLogIndex()) {
                    lastAppliedTerm = raftLog.getEntryTerm(lastAppliedIndex);
                }
                localConfiguration.mergeFrom(configuration);
            } finally {
                lock.unlock();
            }

            boolean success = false;
            snapshot.getLock().lock();
            try {
                LOG.info("start taking snapshot");
                // take snapshot
                String tmpSnapshotDir = snapshot.getSnapshotDir() + ".tmp";
                snapshot.updateMetaData(tmpSnapshotDir, localLastAppliedIndex,
                        lastAppliedTerm, localConfiguration.build());
                String tmpSnapshotDataDir = tmpSnapshotDir + File.separator + "data";
                stateMachine.writeSnapshot(tmpSnapshotDataDir);
                // rename tmp snapshot dir to snapshot dir
                try {
                    File snapshotDirFile = new File(snapshot.getSnapshotDir());
                    if (snapshotDirFile.exists()) {
                        FileUtils.deleteDirectory(snapshotDirFile);
                    }
                    FileUtils.moveDirectory(new File(tmpSnapshotDir),
                            new File(snapshot.getSnapshotDir()));
                    LOG.info("end taking snapshot, result=success");
                    success = true;
                } catch (IOException ex) {
                    LOG.warn("move direct failed when taking snapshot, msg={}", ex.getMessage());
                }
            } finally {
                snapshot.getLock().unlock();
            }

            if (success) {
                // 重新加载snapshot
                long lastSnapshotIndex = 0;
                snapshot.getLock().lock();
                try {
                    snapshot.reload();
                    lastSnapshotIndex = snapshot.getMetaData().getLastIncludedIndex();
                } finally {
                    snapshot.getLock().unlock();
                }

                // discard old log entries
                lock.lock();
                try {
                    if (lastSnapshotIndex > 0 && raftLog.getFirstLogIndex() <= lastSnapshotIndex) {
                        raftLog.truncatePrefix(lastSnapshotIndex + 1);
                    }
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            snapshot.getIsTakeSnapshot().compareAndSet(true, false);
        }
    }

    // in lock
    public void applyConfiguration(RaftProto.LogEntry entry) {
        try {
            RaftProto.Configuration newConfiguration
                    = RaftProto.Configuration.parseFrom(entry.getData().toByteArray());
            configuration = newConfiguration;
            // update peerMap
            for (RaftProto.Server server : newConfiguration.getServersList()) {
                if (!peerMap.containsKey(server.getServerId())
                        && server.getServerId() != localServer.getServerId()) {
                    Peer peer = new Peer(server);
                    peer.setNextIndex(raftLog.getLastLogIndex() + 1);
                    peerMap.put(server.getServerId(), peer);
                }
            }
            LOG.info("new conf is {}, leaderId={}", jsonFormat.printToString(newConfiguration), leaderId);
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
    }
    // 尝试获取最后一条日志项，如果日志不为空，就从日志中获取，否则获取快照的最后一条日志项的任期号
    public long getLastLogTerm() {
        long lastLogIndex = raftLog.getLastLogIndex();  // 获取最后一条索引
        if (lastLogIndex >= raftLog.getFirstLogIndex()) {   // 如果能够确保不会获取到过期日志项
            return raftLog.getEntryTerm(lastLogIndex);
        } else {
            // log为空，lastLogIndex == lastSnapshotIndex
            return snapshot.getMetaData().getLastIncludedTerm();    // 如果日志为空，那么最后一条日志项就是快照的最后一条日志项
        }
    }

    /**
     * 选举定时器
     */ // 如果 electionScheduledFuture 不为空，那么取消任务，重新提交一个选举任务，超时时间是随机的
    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) { // 如果有任务正在执行，需要取消其中的任务
            electionScheduledFuture.cancel(true);
        }   // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
        electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                startPreVote();
            }   // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
        }, getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
    }
    // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = raftOptions.getElectionTimeoutMilliseconds()    // 默认是 5000 ms
                + random.nextInt(0, raftOptions.getElectionTimeoutMilliseconds());
        LOG.debug("new election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    /**
     * 客户端发起pre-vote请求。
     * pre-vote/vote是典型的二阶段实现。
     * 作用是防止某一个节点断网后，不断的增加term发起投票；
     * 当该节点网络恢复后，会导致集群其他节点的term增大，导致集群状态变更。
     */
    private void startPreVote() {
        lock.lock();
        try {   // 如果配置中不包含自身，那么就不再继续后边的流程（本次的任务会被 cancel）
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            LOG.info("Running pre-vote in term {}", currentTerm);
            state = NodeState.STATE_PRE_CANDIDATE;  // 更新状态为预候选人态
        } finally {
            lock.unlock();
        }

        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == localServer.getServerId()) {    // 配置中的节点，忽略自己
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());    // 获取对应的 Peer 实例
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    preVote(peer);
                }
            });
        }
        resetElectionTimer();
    }

    /**
     * 客户端发起正式vote，对candidate有效
     */
    private void startVote() {  // 这时节点的身份变成了 candidate
        lock.lock();
        try {
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) { // 如果自己已经不在配置项中，那么重置流程
                resetElectionTimer();
                return;
            }
            currentTerm++;  // 任期自增
            LOG.info("Running for election in term {}", currentTerm);
            state = NodeState.STATE_CANDIDATE;  // 标记自己的当前状态为 candidate
            leaderId = 0;
            votedFor = localServer.getServerId();   // 声明自己给自己投票了，那么就不会再给别人投票
        } finally {
            lock.unlock();
        }

        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == localServer.getServerId()) {
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());    // 请求其它节点为自己投票
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    requestVote(peer);  // 请求其它节点为自己投票
                }
            });
        }
    }

    /**
     * 客户端发起pre-vote请求
     * @param peer 服务端节点信息
     */
    private void preVote(Peer peer) {
        LOG.info("begin pre vote request");
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);  // 构建的 VoteRequest 里边包含了必要的投票请求参数
            requestBuilder.setServerId(localServer.getServerId())   // 自己的 ServerId
                    .setTerm(currentTerm)   // 当前的任期号
                    .setLastLogIndex(raftLog.getLastLogIndex()) // 自己的最新日志索引号
                    .setLastLogTerm(getLastLogTerm());  // 尝试获取最后一条日志项，如果日志不为空，就从日志中获取，否则获取快照的最后一条日志项
        } finally {
            lock.unlock();
        }

        RaftProto.VoteRequest request = requestBuilder.build(); // 构建真正的 VoteRequest
        peer.getRaftConsensusServiceAsync().preVote(    // 通过代理类进行真正的 rpc 通信
                request, new PreVoteResponseCallback(peer, request));   // request 是传输的内容
    }

    /**
     * 客户端发起正式vote请求
     * @param peer 服务端节点信息
     */
    private void requestVote(Peer peer) {   // 请求其它节点为自己投票
        LOG.info("begin vote request");
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();  // 此处构建的投票请求
        lock.lock();
        try {
            peer.setVoteGranted(null);  // 初始化投票的结果（置空）
            requestBuilder.setServerId(localServer.getServerId())   // 自己的唯一 id
                    .setTerm(currentTerm)   // 当前任期号
                    .setLastLogIndex(raftLog.getLastLogIndex()) // 自己最新一条日志项的索引
                    .setLastLogTerm(getLastLogTerm());  // 自己最新一条日志项的任期号
        } finally {
            lock.unlock();
        }

        RaftProto.VoteRequest request = requestBuilder.build(); // 构建真正的 VoteRequest
        peer.getRaftConsensusServiceAsync().requestVote(
                request, new VoteResponseCallback(peer, request));
    }

    private class PreVoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {
        private Peer peer;
        private RaftProto.VoteRequest request;
        // 保存了 Peer 和 VoteRequest
        public PreVoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted()); // 将其它节点的投票结果进行标记
                if (currentTerm != request.getTerm() || state != NodeState.STATE_PRE_CANDIDATE) {
                    LOG.info("ignore preVote RPC result");
                    return;
                }
                if (response.getTerm() > currentTerm) { // 如果别人的任期号大于自己的任期号
                    LOG.info("Received pre vote response from server {} " +
                                    "in term {} (this server's term was {})",
                            peer.getServer().getServerId(),
                            response.getTerm(),
                            currentTerm);
                    stepDown(response.getTerm());   // 别人的任期号比自己的大，需要降级
                } else {
                    if (response.getGranted()) {    // 如果这个节点投的是赞成票
                        LOG.info("get pre vote granted from server {} for term {}",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 1;
                        for (RaftProto.Server server : configuration.getServersList()) {
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            Peer peer1 = peerMap.get(server.getServerId());  // 这里是获取其它节点的投票情况
                            if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {   // 如果其它节点给自己投了赞成票
                                voteGrantedNum += 1;    // 票数增加
                            }
                        }
                        LOG.info("preVoteGrantedNum={}", voteGrantedNum);   // 输出得票数
                        if (voteGrantedNum > configuration.getServersCount() / 2) { // 如果得票数超过半数
                            LOG.info("get majority pre vote, serverId={} when pre vote, start vote",
                                    localServer.getServerId());
                            startVote();    // 这才是真正的投票，之前只是为了避免 term 不断上涨
                        }
                    } else {
                        LOG.info("pre vote denied by server {} with term {}, my term is {}",
                                peer.getServer().getServerId(), response.getTerm(), currentTerm);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("pre vote with peer[{}:{}] failed",
                    peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            peer.setVoteGranted(new Boolean(false));
        }
    }

    private class VoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {
        private Peer peer;
        private RaftProto.VoteRequest request;

        public VoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted()); // 标记其它节点的投票结果
                if (currentTerm != request.getTerm() || state != NodeState.STATE_CANDIDATE) {
                    LOG.info("ignore requestVote RPC result");  // 如果自己当前的任期号和发起请求的任期号不同，或者自己不是候选人身份，需要忽略本次的投票结果
                    return;
                }
                if (response.getTerm() > currentTerm) { // 如果响应的任期号比自己的高
                    LOG.info("Received RequestVote response from server {} " +
                                    "in term {} (this server's term was {})",
                            peer.getServer().getServerId(),
                            response.getTerm(),
                            currentTerm);
                    stepDown(response.getTerm());   // 降级
                } else {
                    if (response.getGranted()) {    // 如果收到的是赞成票
                        LOG.info("Got vote from server {} for term {}",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 0; // 统计得票数
                        if (votedFor == localServer.getServerId()) {    // 将自己标记为给自己投票了，不再给其它节点投票
                            voteGrantedNum += 1;    // 得票数自增
                        }
                        for (RaftProto.Server server : configuration.getServersList()) {    // 统计其它节点给自己的投票情况
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            Peer peer1 = peerMap.get(server.getServerId()); // 统计其它节点给自己的投票情况
                            if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {
                                voteGrantedNum += 1;    // 如果其它节点给自己投了赞成票，票数自增
                            }
                        }
                        LOG.info("voteGrantedNum={}", voteGrantedNum);  // 打印得票数
                        if (voteGrantedNum > configuration.getServersCount() / 2) { // 如果得票数超过了半数
                            LOG.info("Got majority vote, serverId={} become leader", localServer.getServerId());    // 自己的得票数超过半数了
                            becomeLeader(); // 执行晋升 Leader 的操作
                        }
                    } else {
                        LOG.info("Vote denied by server {} with term {}, my term is {}",    // 如果收到的是否定票，直接打印日志不做处理
                                peer.getServer().getServerId(), response.getTerm(), currentTerm);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("requestVote with peer[{}:{}] failed",
                    peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            peer.setVoteGranted(new Boolean(false));
        }
    }

    // in lock
    private void becomeLeader() {   // 更新自身的状态，取消正在执行的任务
        state = NodeState.STATE_LEADER; // 更新状态
        leaderId = localServer.getServerId();
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) { // 如果定时任务不为空，且任务未完成，取消正在执行的任务
            electionScheduledFuture.cancel(true);
        }
        // start heartbeat timer
        startNewHeartbeat();
    }

    // heartbeat timer, append entries
    // in lock
    private void resetHeartbeatTimer() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {   // 如果心跳任务不为空，且未完成，那么就取消心跳任务
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {   // 提交心跳任务
            @Override
            public void run() {
                startNewHeartbeat();
            }
        }, raftOptions.getHeartbeatPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    // in lock, 开始心跳，对leader有效
    private void startNewHeartbeat() {  // 这里应该就是 Leader 节点发送心跳消息
        LOG.debug("start new heartbeat, peers={}", peerMap.keySet());
        for (final Peer peer : peerMap.values()) {  // 针对每一个节点，提交一个 appendEntries 任务
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    appendEntries(peer);
                }
            });
        }
        resetHeartbeatTimer();  // 重新开始心跳任务
    }

    // in lock, for leader
    private void advanceCommitIndex() { // 根据各个节点的 matchIndex 来计算得到 commitIndex（超过半数的匹配即可提交），本机执行日志项到 commitIndex，更新 applyIndex
        // 获取quorum matchIndex
        int peerNum = configuration.getServersList().size();    // 节点数
        long[] matchIndexes = new long[peerNum];    // 保存其它节点的配置日志项索引
        int i = 0;
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() != localServer.getServerId()) {
                Peer peer = peerMap.get(server.getServerId());
                matchIndexes[i++] = peer.getMatchIndex();
            }
        }
        matchIndexes[i] = raftLog.getLastLogIndex();    // 自己的匹配日志项就是日志中的最后一条
        Arrays.sort(matchIndexes);  // 排序所有匹配的日志项
        long newCommitIndex = matchIndexes[peerNum / 2];    // 取中间的结果作为全局的提交值
        LOG.debug("newCommitIndex={}, oldCommitIndex={}", newCommitIndex, commitIndex); // 新的 commitIndex
        if (raftLog.getEntryTerm(newCommitIndex) != currentTerm) {  // 如果被提交的日志项的任期不是自己的任期，直接返回，可能出现了问题
            LOG.debug("newCommitIndexTerm={}, currentTerm={}",
                    raftLog.getEntryTerm(newCommitIndex), currentTerm);
            return;
        }

        if (commitIndex >= newCommitIndex) {    // 新的提交索引反而比之前的小，直接返回
            return;
        }
        long oldCommitIndex = commitIndex;
        commitIndex = newCommitIndex;   // 更新 commitIndex
        // 同步到状态机
        for (long index = oldCommitIndex + 1; index <= newCommitIndex; index++) {
            RaftProto.LogEntry entry = raftLog.getEntry(index); // 获取日志项
            if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
                stateMachine.apply(entry.getData().toByteArray());  // 如果是数据类型的日志项，交由状态机执行
            } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
                applyConfiguration(entry);  // 如果是配置类型的日志项，直接应用
            }
        }
        lastAppliedIndex = commitIndex; // 更新 applyIndex
        LOG.debug("commitIndex={} lastAppliedIndex={}", commitIndex, lastAppliedIndex);
        commitIndexCondition.signalAll();
    }

    // in lock  // 将多条日志项打包到 AppendEntriesRequest 中，返回打包的数量
    private long packEntries(long nextIndex, RaftProto.AppendEntriesRequest.Builder requestBuilder) {
        long lastIndex = Math.min(raftLog.getLastLogIndex(),    // 本次追加日志项的最大索引（lastIndex），不能超过 raftLog 最后一个日志项
                nextIndex + raftOptions.getMaxLogEntriesPerRequest() - 1);
        for (long index = nextIndex; index <= lastIndex; index++) {
            RaftProto.LogEntry entry = raftLog.getEntry(index);
            requestBuilder.addEntries(entry);   // 添加日志项
        }
        return lastIndex - nextIndex + 1;   // 返回批量操作的日志项数
    }

    private boolean installSnapshot(Peer peer) {
        if (snapshot.getIsTakeSnapshot().get()) {
            LOG.info("already in take snapshot, please send install snapshot request later");
            return false;
        }
        if (!snapshot.getIsInstallSnapshot().compareAndSet(false, true)) {
            LOG.info("already in install snapshot");
            return false;
        }

        LOG.info("begin send install snapshot request to server={}", peer.getServer().getServerId());
        boolean isSuccess = true;
        TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap = snapshot.openSnapshotDataFiles();
        LOG.info("total snapshot files={}", snapshotDataFileMap.keySet());
        try {
            boolean isLastRequest = false;
            String lastFileName = null;
            long lastOffset = 0;
            long lastLength = 0;
            while (!isLastRequest) {
                RaftProto.InstallSnapshotRequest request
                        = buildInstallSnapshotRequest(snapshotDataFileMap, lastFileName, lastOffset, lastLength);
                if (request == null) {
                    LOG.warn("snapshot request == null");
                    isSuccess = false;
                    break;
                }
                if (request.getIsLast()) {
                    isLastRequest = true;
                }
                LOG.info("install snapshot request, fileName={}, offset={}, size={}, isFirst={}, isLast={}",
                        request.getFileName(), request.getOffset(), request.getData().toByteArray().length,
                        request.getIsFirst(), request.getIsLast());
                RaftProto.InstallSnapshotResponse response
                        = peer.getRaftConsensusServiceAsync().installSnapshot(request);
                if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
                    lastFileName = request.getFileName();
                    lastOffset = request.getOffset();
                    lastLength = request.getData().size();
                } else {
                    isSuccess = false;
                    break;
                }
            }

            if (isSuccess) {
                long lastIncludedIndexInSnapshot;
                snapshot.getLock().lock();
                try {
                    lastIncludedIndexInSnapshot = snapshot.getMetaData().getLastIncludedIndex();
                } finally {
                    snapshot.getLock().unlock();
                }

                lock.lock();
                try {
                    peer.setNextIndex(lastIncludedIndexInSnapshot + 1);
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            snapshot.closeSnapshotDataFiles(snapshotDataFileMap);
            snapshot.getIsInstallSnapshot().compareAndSet(true, false);
        }
        LOG.info("end send install snapshot request to server={}, success={}",
                peer.getServer().getServerId(), isSuccess);
        return isSuccess;
    }

    private RaftProto.InstallSnapshotRequest buildInstallSnapshotRequest(
            TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap,
            String lastFileName, long lastOffset, long lastLength) {
        RaftProto.InstallSnapshotRequest.Builder requestBuilder = RaftProto.InstallSnapshotRequest.newBuilder();

        snapshot.getLock().lock();
        try {
            if (lastFileName == null) {
                lastFileName = snapshotDataFileMap.firstKey();
                lastOffset = 0;
                lastLength = 0;
            }
            Snapshot.SnapshotDataFile lastFile = snapshotDataFileMap.get(lastFileName);
            long lastFileLength = lastFile.randomAccessFile.length();
            String currentFileName = lastFileName;
            long currentOffset = lastOffset + lastLength;
            int currentDataSize = raftOptions.getMaxSnapshotBytesPerRequest();
            Snapshot.SnapshotDataFile currentDataFile = lastFile;
            if (lastOffset + lastLength < lastFileLength) {
                if (lastOffset + lastLength + raftOptions.getMaxSnapshotBytesPerRequest() > lastFileLength) {
                    currentDataSize = (int) (lastFileLength - (lastOffset + lastLength));
                }
            } else {
                Map.Entry<String, Snapshot.SnapshotDataFile> currentEntry
                        = snapshotDataFileMap.higherEntry(lastFileName);
                if (currentEntry == null) {
                    LOG.warn("reach the last file={}", lastFileName);
                    return null;
                }
                currentDataFile = currentEntry.getValue();
                currentFileName = currentEntry.getKey();
                currentOffset = 0;
                int currentFileLenght = (int) currentEntry.getValue().randomAccessFile.length();
                if (currentFileLenght < raftOptions.getMaxSnapshotBytesPerRequest()) {
                    currentDataSize = currentFileLenght;
                }
            }
            byte[] currentData = new byte[currentDataSize];
            currentDataFile.randomAccessFile.seek(currentOffset);
            currentDataFile.randomAccessFile.read(currentData);
            requestBuilder.setData(ByteString.copyFrom(currentData));
            requestBuilder.setFileName(currentFileName);
            requestBuilder.setOffset(currentOffset);
            requestBuilder.setIsFirst(false);
            if (currentFileName.equals(snapshotDataFileMap.lastKey())
                    && currentOffset + currentDataSize >= currentDataFile.randomAccessFile.length()) {
                requestBuilder.setIsLast(true);
            } else {
                requestBuilder.setIsLast(false);
            }
            if (currentFileName.equals(snapshotDataFileMap.firstKey()) && currentOffset == 0) {
                requestBuilder.setIsFirst(true);
                requestBuilder.setSnapshotMetaData(snapshot.getMetaData());
            } else {
                requestBuilder.setIsFirst(false);
            }
        } catch (Exception ex) {
            LOG.warn("meet exception:", ex);
            return null;
        } finally {
            snapshot.getLock().unlock();
        }

        lock.lock();
        try {
            requestBuilder.setTerm(currentTerm);
            requestBuilder.setServerId(localServer.getServerId());
        } finally {
            lock.unlock();
        }

        return requestBuilder.build();
    }

    public Lock getLock() {
        return lock;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public SegmentedLog getRaftLog() {
        return raftLog;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public RaftProto.Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(RaftProto.Configuration configuration) {
        this.configuration = configuration;
    }

    public RaftProto.Server getLocalServer() {
        return localServer;
    }

    public NodeState getState() {
        return state;
    }

    public ConcurrentMap<Integer, Peer> getPeerMap() {
        return peerMap;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Condition getCatchUpCondition() {
        return catchUpCondition;
    }
}
