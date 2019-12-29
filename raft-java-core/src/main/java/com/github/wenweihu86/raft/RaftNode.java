package com.github.wenweihu86.raft;

import com.baidu.brpc.client.RpcCallback;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.storage.SegmentedLog;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.google.protobuf.ByteString;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    // 当前节点是否进入 Leader 选举阶段
    private volatile boolean electionLeader = false;
    // 资格确认标识符
    private volatile boolean qualificationConfirmOk = false;
    // 资格写入标识符
    private boolean qualificationWriteOk;

    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = lock.newCondition();   // 两个 condition 量
    private Condition catchUpCondition = lock.newCondition();
    // 优先级表
    private List<Integer> priorityTable = new ArrayList<>();

    private Set<Integer> qualificationTable = Collections.synchronizedSet(new HashSet<Integer>());

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    // 将本地的日志段文件载入到 SegmentedLog，读取本地的快照，通过本地快照恢复 currentTerm、votedFor、commitIndex，
    public RaftNode(RaftOptions raftOptions,    // 并砍掉快照之前的全部日志项，应用本地快照的数据，和之后的索引项
                    List<RaftProto.Server> servers,
                    RaftProto.Server localServer,
                    StateMachine stateMachine) {
        this.raftOptions = raftOptions;
        RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder(); // 协议的 Configuration，protobuf 生成
        for (RaftProto.Server server : servers) {
            confBuilder.addServers(server);
            priorityTable.add(server.getServerId());
        }
        Collections.sort(priorityTable);
        configuration = confBuilder.build();

        this.localServer = localServer;
        this.stateMachine = stateMachine;

        // load log and snapshot 尝试加载段数据（本地），加载了本地的元数据，将日志段数据解析为 Segment，然后更新了元数据信息
        raftLog = new SegmentedLog(raftOptions.getDataDir(), raftOptions.getMaxSegmentFileSize());  // 尝试加载段数据（本地），加载了本地的元数据
        snapshot = new Snapshot(raftOptions.getDataDir());  // 创建快照类，主要是创建了快照对应的目录
        snapshot.reload();  // 尝试从本地获取快照元数据，没有的话就构建一个

        currentTerm = raftLog.getMetaData().getCurrentTerm();   // 获取日志元数据中的任期号
        votedFor = raftLog.getMetaData().getVotedFor(); // 获取日志元数据中的投票值
        commitIndex = Math.max(snapshot.getMetaData().getLastIncludedIndex(), commitIndex); // 确定 commit index
        // discard old log entries 丢弃过期日志项
        if (snapshot.getMetaData().getLastIncludedIndex() > 0   // 如果进行过快照，且本机还包含快照过的日志项
                && raftLog.getFirstLogIndex() <= snapshot.getMetaData().getLastIncludedIndex()) {
            raftLog.truncatePrefix(snapshot.getMetaData().getLastIncludedIndex() + 1);  // 删除快照最后一条日志项之前的全部日志项
        }
        // apply state machine
        RaftProto.Configuration snapshotConfiguration = snapshot.getMetaData().getConfiguration();  // 快照元数据中的配置信息
        if (snapshotConfiguration.getServersCount() > 0) {  // 如果元数据的配置的节点数大于 0，就是用这个配置信息
            configuration = snapshotConfiguration;
        }
        String snapshotDataDir = snapshot.getSnapshotDir() + File.separator + "data";   // 快照数据目录
        stateMachine.readSnapshot(snapshotDataDir); // 通过状态机读取快照，如果 rocksdb 文件夹存在，删除，然后将快照的文件夹拷贝到 rocksdb 中，最后构建 RocksDB，打开位置为 rocksdb 目录
        for (long index = snapshot.getMetaData().getLastIncludedIndex() + 1;
             index <= commitIndex; index++) {   // 如果快照之后到 commitIndex 之间有剩余的日志项
            RaftProto.LogEntry entry = raftLog.getEntry(index); // 获取 index 所在的 Segment，然后从中获取 index 对应的 LogEntry
            if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {   // 如果这个日志项是数据，那么就交由状态机执行
                stateMachine.apply(entry.getData().toByteArray());  // 将 dataBytes 解析为 SetRequest，以 key-value 的形式存入 RocksDB
            } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {   // 如果是配置项块，应用配置项
                applyConfiguration(entry);  // 将 LogEntry 解析为 Configuration，然后将新加入的节点包装为 Peer 实例，然后保存到 peerMap 中
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
        executorService = new ThreadPoolExecutor(   // 构建一个必要的线程池
                raftOptions.getRaftConsensusThreadNum(),
                raftOptions.getRaftConsensusThreadNum(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        scheduledExecutorService = Executors.newScheduledThreadPool(2); // 构建一个定时任务
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {    // 提交一个定时快照任务
            @Override
            public void run() {
                takeSnapshot();
            }
        }, raftOptions.getSnapshotPeriodSeconds(), raftOptions.getSnapshotPeriodSeconds(), TimeUnit.SECONDS);
        // start election
        resetElectionTimer();   // 开启 Leader 竞选流程
    }

    // client set command 将待写入的数据构建为 LogEntry，然后批量追加到日志文件中，最后等待日志项被应用，返回日志项应用的结果
    public boolean replicate(byte[] data, RaftProto.EntryType entryType) {
        lock.lock();
        long newLastLogIndex = 0;
        try {
            if (state != NodeState.STATE_LEADER) {  // 只有 Leader 才有资格进行日志复制的操作
                LOG.debug("I'm not the leader");
                return false;
            }
            RaftProto.LogEntry logEntry = RaftProto.LogEntry.newBuilder()   // 构建日志项任务
                    .setTerm(currentTerm)   // 设置任期
                    .setType(entryType) // 设置消息类型
                    .setData(ByteString.copyFrom(data)).build();    // 将数据填充到 LogEntry 中
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            entries.add(logEntry);  // list 装 LogEntry
            newLastLogIndex = raftLog.append(entries);  // 判断是否需要新的日志段文件，然后将日志项写入到日志段文件中，更新对应的记录信息，返回写入后新的 LastLogIndex
            // 创建元数据文件，将元数据信息写入元数据文件中 currentTerm=18, votedFor=2, firstLogIndex=1
            raftLog.updateMetaData(currentTerm, null, raftLog.getFirstLogIndex());
            for (RaftProto.Server server : configuration.getServersList()) {
                final Peer peer = peerMap.get(server.getServerId());
                executorService.submit(new Runnable() { // 向其它节点提交追加日志项的任务
                    @Override
                    public void run() {
                        appendEntries(peer);
                    }
                });
            }

            if (raftOptions.isAsyncWrite()) {   // 如果是异步写入，那么主节点写入成功后即可返回（可能会存在安全问题）
                // 主节点写成功后，就返回。
                return true;
            }

            // sync wait commitIndex >= newLastLogIndex
            long startTime = System.currentTimeMillis();    // 当前时间戳
            while (lastAppliedIndex < newLastLogIndex) {    // 所以这里退出循环的条件是 applyIndex 追赶上 commitIndex 或者等待超时
                if (System.currentTimeMillis() - startTime >= raftOptions.getMaxAwaitTimeout()) {
                    break;  // 超过等待时间，直接 break
                }   // 等待日志项被执行
                commitIndexCondition.await(raftOptions.getMaxAwaitTimeout(), TimeUnit.MILLISECONDS);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            lock.unlock();
        }
        LOG.debug("lastAppliedIndex={} newLastLogIndex={}", lastAppliedIndex, newLastLogIndex);
        if (lastAppliedIndex < newLastLogIndex) {   // 如果 applyIndex 还是没有追赶上 commitIndex，那么表示本次的 put 过程失败
            return false;
        }
        return true;
    }

    // 追加日志，先判断是否需要安装快照，然后将必要的信息封装为 AppendEntriesRequest，发送对应的 rpc 请求，如果成功，更新 commitIndex 然后执行对应的日志项
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
                        if (raftLog.getLastLogIndex() - peer.getMatchIndex() <= raftOptions.getCatchupMargin()) {   // 这里是判断 peer 的日志项是否追赶上 Leader 节点
                            LOG.debug("peer catch up the leader");
                            peer.setCatchUp(true);  // 标记 peer 已经追赶上 Leader 节点
                            // signal the caller thread
                            catchUpCondition.signalAll();   // 通知日志追赶的任务
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

    // in lock 判断参数是否合理，然后重置一些状态信息，更新自己的身份为 Follower，终止心跳任务，重置选举定时器
    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {    // 参数检查，如果自己的任期比较高，那么就不能执行 stepDown
            LOG.error("can't be happened");
            return;
        }
        if (currentTerm < newTerm) {    // 如果当前任期比它人的低
            currentTerm = newTerm;  // 更新自己的任期为跟它人一样
            leaderId = 0;   // 重置 Leader
            votedFor = 0;   // 重置投票结果
            raftLog.updateMetaData(currentTerm, votedFor, null);    // 更新元数据信息
        }
        state = NodeState.STATE_FOLLOWER;   // 更新当前状态为 Follower
        // stop heartbeat
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);  // 终止定时的心跳任务
        }
        resetElectionTimer();   // 重置选举定时器
    }

    // 拍快照，检查当前是否满足拍快照的条件，拍快照，然后将结果替换原快照的内容，然后重新载入快照元信息，并删除过期的日志项
    public void takeSnapshot() {
        if (snapshot.getIsInstallSnapshot().get()) {    // 如果正在安装快照，忽略拍快照
            LOG.info("already in install snapshot, ignore take snapshot");
            return;
        }

        snapshot.getIsTakeSnapshot().compareAndSet(false, true);    // 设置正在拍快照
        try {
            long localLastAppliedIndex;
            long lastAppliedTerm = 0;
            RaftProto.Configuration.Builder localConfiguration = RaftProto.Configuration.newBuilder();  // 构建 Configuration
            lock.lock();
            try {
                if (raftLog.getTotalSize() < raftOptions.getSnapshotMinLogSize()) { // 如果日志的总大小没达到拍快照的门槛
                    return; // 直接返回
                }
                if (lastAppliedIndex <= snapshot.getMetaData().getLastIncludedIndex()) {
                    return; // 如果 applyIndex 还没将快照执行完，直接返回
                }
                localLastAppliedIndex = lastAppliedIndex;
                if (lastAppliedIndex >= raftLog.getFirstLogIndex()
                        && lastAppliedIndex <= raftLog.getLastLogIndex()) {
                    lastAppliedTerm = raftLog.getEntryTerm(lastAppliedIndex);   // 获取当前 applyIndex 日志项的任期号
                }
                localConfiguration.mergeFrom(configuration);    // 配置信息的合并
            } finally {
                lock.unlock();
            }

            boolean success = false;
            snapshot.getLock().lock();  // 获取拍快照的锁
            try {
                LOG.info("start taking snapshot");
                // take snapshot
                String tmpSnapshotDir = snapshot.getSnapshotDir() + ".tmp"; // 临时快照文件路径
                snapshot.updateMetaData(tmpSnapshotDir, localLastAppliedIndex,  // 即将最后包含的日志项索引、日志项任期、集群节点信息保存到快照元数据文件中
                        lastAppliedTerm, localConfiguration.build());   // 更新 snapshot 元数据信息
                String tmpSnapshotDataDir = tmpSnapshotDir + File.separator + "data";   // 元数据数据文件夹
                stateMachine.writeSnapshot(tmpSnapshotDataDir); // 创建 RocksDB 的快照，然后将结果保存到快照目录
                // rename tmp snapshot dir to snapshot dir
                try {
                    File snapshotDirFile = new File(snapshot.getSnapshotDir()); // 快照文件夹
                    if (snapshotDirFile.exists()) {
                        FileUtils.deleteDirectory(snapshotDirFile); // 如果快照文件夹存在，删除快照文件夹
                    }
                    FileUtils.moveDirectory(new File(tmpSnapshotDir),   // 重命名快照文件夹
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
                snapshot.getLock().lock();  // 获取快照锁
                try {
                    snapshot.reload();  // 尝试从本地获取快照元数据，没有的话就构建一个
                    lastSnapshotIndex = snapshot.getMetaData().getLastIncludedIndex();  // 获取当前快照的最后一个日志项索引
                } finally {
                    snapshot.getLock().unlock();
                }

                // discard old log entries
                lock.lock();
                try {   // 如果快照的最后日志项索引位于日志的第一条日志项之前，删除之前的日志项
                    if (lastSnapshotIndex > 0 && raftLog.getFirstLogIndex() <= lastSnapshotIndex) {
                        raftLog.truncatePrefix(lastSnapshotIndex + 1);  // 删除之前的日志项，根据快照的结果，删除 newFirstIndex 之前的过期日志项，然后更新日志的元数据信息
                    }
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            snapshot.getIsTakeSnapshot().compareAndSet(true, false);    // 将正在拍快照的状态修改为 false
        }
    }

    // in lock 将 LogEntry 解析为 Configuration，然后将新加入的节点包装为 Peer 实例，然后保存到 peerMap 中
    public void applyConfiguration(RaftProto.LogEntry entry) {
        try {
            RaftProto.Configuration newConfiguration    // 将 LogEntry 解析为 Configuration
                    = RaftProto.Configuration.parseFrom(entry.getData().toByteArray());
            configuration = newConfiguration;
            // update peerMap
            for (RaftProto.Server server : newConfiguration.getServersList()) {
                if (!peerMap.containsKey(server.getServerId())  // 如果 peerMap 不包含新配置项中的节点
                        && server.getServerId() != localServer.getServerId()) { // 且该节点的 id 不是自己，也就是新加入的节点
                    Peer peer = new Peer(server);   // 构建一个新的 Peer
                    peer.setNextIndex(raftLog.getLastLogIndex() + 1);   // 指定新添加的节点的 nextIndex
                    peerMap.put(server.getServerId(), peer);    // 保存到 peerMap 中
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
    public void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) { // 如果有任务正在执行，需要取消其中的任务
            electionScheduledFuture.cancel(true);
        }   // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
        if (!raftOptions.isPriorityElection()) {
            electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    startPreVote();
                }   // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
            }, getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
        } else {
            electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    startPrepareElection();
                }
            }, getPriorityElectionTimeoutMs(), TimeUnit.MILLISECONDS);
        }
    }

    public void startPrepareElection() {
        lock.lock();
        try {   // 如果配置中不包含自身，那么就不再继续后边的流程（本次的任务会被 cancel）
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            LOG.info("Start prepare election in term {}", currentTerm);
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
                    prepareElection(peer);
                }
            });
        }
        resetElectionTimer();
    }

    private void prepareElection(Peer peer) {
        LOG.info("begin prepare start vote request");
        RaftProto.PrepareElectionRequest.Builder requestBuilder = RaftProto.PrepareElectionRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
        } finally {
            lock.unlock();
        }

        RaftProto.PrepareElectionRequest request = requestBuilder.build(); // 构建真正的 VoteRequest
        peer.getRaftConsensusServiceAsync().prepareElection(    // 通过代理类进行真正的 rpc 通信
                request, new PrepareElectionResponseCallback(peer, request));   // request 是传输的内容
    }

    // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = raftOptions.getElectionTimeoutMilliseconds()    // 默认是 5000 ms
                + random.nextInt(0, raftOptions.getElectionTimeoutMilliseconds());
        LOG.debug("new election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    // 获取一个随机超时时间，为选举超时时间加上 0 ~ electionTimeout 之间的数
    private int getPriorityElectionTimeoutMs() {
        int priorityTimeout = raftOptions.getElectionTimeoutMilliseconds();    // 默认是 5000 ms
        LOG.debug("new election time is after {} ms", priorityTimeout);
        return priorityTimeout;
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
     *
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
     *
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
                    stepDown(response.getTerm());   // 别人的任期号比自己的大，需要降级，就是更新自己的任期号、同时取消自己设置的心跳任务
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

    // 准备选举响应的回调
    private class PrepareElectionResponseCallback implements RpcCallback<RaftProto.PrepareElectionResponse> {

        private final Peer peer;
        private final RaftProto.PrepareElectionRequest request;

        public PrepareElectionResponseCallback(Peer peer, RaftProto.PrepareElectionRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override   // 准备选举响应成功的回调函数
        public void success(RaftProto.PrepareElectionResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted()); // 将其它节点的投票结果进行标记
                if (response.getGranted()) {    // 如果这个节点投的是赞成票
                    LOG.info("get prepare election granted from server {} for term {}",
                            peer.getServer().getServerId(), currentTerm);
                    int prepareElectionGrantedNum = 1;
                    for (RaftProto.Server server : configuration.getServersList()) {
                        if (server.getServerId() == localServer.getServerId()) {
                            continue;
                        }
                        Peer peer1 = peerMap.get(server.getServerId());  // 这里是获取其它节点的投票情况
                        if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {   // 如果其它节点给自己投了赞成票
                            prepareElectionGrantedNum += 1;    // 票数增加
                        }
                    }
                    LOG.info("prepareElectionGrantedNum={}", prepareElectionGrantedNum);   // 输出得票数
                    if (prepareElectionGrantedNum > configuration.getServersCount() / 2) { // 如果得票数超过半数
                        LOG.info("get majority grants, serverId={} when prepare election, start election",
                                localServer.getServerId());
                    }
                } else {
                    LOG.info("prepare election denied by server {}, my term is {}",
                            peer.getServer().getServerId(), currentTerm);
                }
            } finally {
                lock.unlock();
            }
            startQualificationConfirm();    // 资格确认阶段
        }

        @Override
        public void fail(Throwable e) {

        }
    }

    // 资格确认响应回调
    private class QualificationConfirmResponseCallback implements RpcCallback<RaftProto.QualificationConfirmResponse> {
        private final Peer peer;
        private final RaftProto.QualificationConfirmRequest request;
        private final RaftNode raftNode;

        public QualificationConfirmResponseCallback(RaftNode raftNode, Peer peer, RaftProto.QualificationConfirmRequest request) {
            this.peer = peer;
            this.request = request;
            this.raftNode = raftNode;
        }

        @Override   // 资格确认响应成功回调函数
        public void success(RaftProto.QualificationConfirmResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted()); // 将其它节点的投票结果进行标记
                if (response.getGranted()) {    // 如果这个节点投的是赞成票
                    LOG.info("get qualification confirm granted from server {} for term {}",
                            peer.getServer().getServerId(), currentTerm);
                    int qualificationConfirmGrantedNum = 1;
                    for (RaftProto.Server server : configuration.getServersList()) {
                        if (server.getServerId() == localServer.getServerId()) {
                            continue;
                        }
                        Peer peer1 = peerMap.get(server.getServerId());  // 这里是获取其它节点的投票情况
                        if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {   // 如果其它节点给自己投了赞成票
                            qualificationConfirmGrantedNum += 1;    // 票数增加
                        }
                    }
                    LOG.info("qualificationConfirmGrantedNum={}", qualificationConfirmGrantedNum);   // 输出得票数
                    if (qualificationConfirmGrantedNum > configuration.getServersCount() / 2) { // 如果得票数超过半数
                        LOG.info("get majority grants, serverId={} when prepare election, start election",
                                localServer.getServerId());
                        this.raftNode.qualificationConfirmOk = true;
                    }
                } else {
                    LOG.info("qualification confirm denied by server {}, my term is {}",
                            peer.getServer().getServerId(), currentTerm);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {

        }
    }

    // 资格写入回调
    private class QualificationWriteResponseCallback implements RpcCallback<RaftProto.QualificationWriteResponse> {
        private final Peer peer;
        private final RaftProto.QualificationWriteRequest request;
        private final RaftNode raftNode;

        public QualificationWriteResponseCallback(RaftNode raftNode, Peer peer, RaftProto.QualificationWriteRequest request) {
            this.peer = peer;
            this.request = request;
            this.raftNode = raftNode;
        }

        @Override   // 资格写入成功回调
        public void success(RaftProto.QualificationWriteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted()); // 将其它节点的投票结果进行标记
                if (response.getGranted()) {    // 如果这个节点投的是赞成票
                    LOG.info("get qualification write granted from server {} for term {}",
                            peer.getServer().getServerId(), currentTerm);
                    int qualificationConfirmGrantedNum = 1;
                    for (RaftProto.Server server : configuration.getServersList()) {
                        if (server.getServerId() == localServer.getServerId()) {
                            continue;
                        }
                        Peer peer1 = peerMap.get(server.getServerId());  // 这里是获取其它节点的投票情况
                        if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {   // 如果其它节点给自己投了赞成票
                            qualificationConfirmGrantedNum += 1;    // 票数增加
                        }
                    }
                    LOG.info("qualificationConfirmGrantedNum={}", qualificationConfirmGrantedNum);   // 输出得票数
                    if (qualificationConfirmGrantedNum > configuration.getServersCount() / 2) { // 如果得票数超过半数
                        LOG.info("get majority grants, serverId={} when prepare election, start election",
                                localServer.getServerId());
                        this.raftNode.qualificationWriteOk = true;
                    }
                } else {
                    LOG.info("prepare election denied by server {}, my term is {}",
                            peer.getServer().getServerId(), currentTerm);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {

        }
    }

    // 优先级投票的回调
    private class PriorityVoteResponseCallback implements RpcCallback<RaftProto.PriorityVoteResponse> {
        private final Peer peer;
        private final RaftProto.PriorityVoteRequest request;
        private final RaftNode raftNode;

        public PriorityVoteResponseCallback(RaftNode raftNode, Peer peer, RaftProto.PriorityVoteRequest request) {
            this.peer = peer;
            this.request = request;
            this.raftNode = raftNode;
        }

        @Override   // 优先级投票成功的回调函数
        public void success(RaftProto.PriorityVoteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted()); // 标记其它节点的投票结果
                if (response.getGranted()) {    // 如果收到的是赞成票
                    LOG.info("Got priority vote response from server {} for term {}",
                            peer.getServer().getServerId(), currentTerm);
                    int voteGrantedNum = 0; // 统计得票数
                    int serverId = localServer.getServerId();
                    if (isHighestPriority(serverId))    // 如果自己的 id 是自己的资格表中优先级最高的节点
                        voteGrantedNum++;   // 投自己一票
                    for (RaftProto.Server server : configuration.getServersList()) {    // 统计其它节点给自己的投票情况
                        if (server.getServerId() == serverId) {
                            continue;
                        }
                        Peer peer1 = peerMap.get(server.getServerId()); // 统计其它节点给自己的投票情况
                        if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {
                            voteGrantedNum += 1;    // 如果其它节点给自己投了赞成票，票数自增
                        }
                    }
                    LOG.info("voteGrantedNum={}", voteGrantedNum);  // 打印得票数
                    if (voteGrantedNum > configuration.getServersCount() / 2) { // 如果得票数超过了半数
                        LOG.info("Got majority vote, serverId={} become leader", serverId);    // 自己的得票数超过半数了
                        becomeLeader(); // 执行晋升 Leader 的操作
                    }
                } else {
                    LOG.info("Vote denied by server {}, my term is {}",    // 如果收到的是否定票，直接打印日志不做处理
                            peer.getServer().getServerId(), currentTerm);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {

        }
    }

    // 资格确认阶段
    public void startQualificationConfirm() {
        long start = System.currentTimeMillis();
        lock.lock();
        setElectionLeader(true);    // 标记自己进入 Leader 竞选阶段
        try {   // 如果配置中不包含自身，那么就不再继续后边的流程（本次的任务会被 cancel）
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            LOG.info("Running qualification confirm in term {}", currentTerm);
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
                    qualificationConfirm(peer);
                }
            });
        }
        long end = System.currentTimeMillis();
        long rest = raftOptions.getQualificationConfirmTimeout() - (end - start);
        try {
            if (rest > 0)
                Thread.sleep(rest);
            startQualificationWrite();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void printLines() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            System.out.println("----------------------------------------------------");
            Thread.sleep(500);
        }
    }

    // 开始资格写入
    private void startQualificationWrite() {
        long start = System.currentTimeMillis();
        lock.lock();
        try {   // 如果配置中不包含自身，那么就不再继续后边的流程（本次的任务会被 cancel）
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            LOG.info("Running pre-vote in term {}", currentTerm);
        } finally {
            lock.unlock();
        }

        if (qualificationConfirmOk) {   // 只有资格确认成功才能向其它节点发送资格写入请求
            qualificationTable.add(localServer.getServerId());  // 将资格信息写入自己的资格表中
            for (RaftProto.Server server : configuration.getServersList()) {
                if (server.getServerId() == localServer.getServerId()) {    // 配置中的节点，忽略自己
                    continue;
                }
                final Peer peer = peerMap.get(server.getServerId());    // 获取对应的 Peer 实例
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        qualificationWrite(peer);   // 向 peer 发起资格写入请求
                    }
                });
            }
        }
        long end = System.currentTimeMillis();
        long rest = raftOptions.getQualificationWriteTimeout() - (end - start);
        try {
            if (rest > 0)
                Thread.sleep(rest);
            startPriorityVote();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 发起资格写入请求
    private void qualificationWrite(Peer peer) {
        LOG.info("begin qualification write request");
        RaftProto.QualificationWriteRequest.Builder requestBuilder = RaftProto.QualificationWriteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);  // 构建的 VoteRequest 里边包含了必要的投票请求参数
            requestBuilder.setServerId(localServer.getServerId());   // 自己的 ServerId
        } finally {
            lock.unlock();
        }
        RaftProto.QualificationWriteRequest request = requestBuilder.build(); // 构建真正的 VoteRequest
        peer.getRaftConsensusServiceAsync().qualificationWrite(    // 通过代理类进行真正的 rpc 通信
                request, new QualificationWriteResponseCallback(this, peer, request));   // request 是传输的内容
    }

    private void startPriorityVote() {
        lock.lock();
        try {   // 如果配置中不包含自身，那么就不再继续后边的流程（本次的任务会被 cancel）
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            LOG.info("Running pre-vote in term {}", currentTerm);
        } finally {
            lock.unlock();
        }

        if (qualificationWriteOk) {   // 只有资格确认成功才能向其它节点发送资格写入请求
            for (RaftProto.Server server : configuration.getServersList()) {
                if (server.getServerId() == localServer.getServerId()) {    // 配置中的节点，忽略自己
                    continue;
                }
                final Peer peer = peerMap.get(server.getServerId());    // 获取对应的 Peer 实例
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        priorityVote(peer);
                    }
                });
            }
        }
    }

    private void priorityVote(Peer peer) {
        LOG.info("begin priority vote request");
        RaftProto.PriorityVoteRequest.Builder requestBuilder = RaftProto.PriorityVoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);  // 构建的 VoteRequest 里边包含了必要的投票请求参数
            requestBuilder.setServerId(localServer.getServerId());   // 自己的 ServerId
        } finally {
            lock.unlock();
        }
        RaftProto.PriorityVoteRequest request = requestBuilder.build(); // 构建真正的 VoteRequest
        peer.getRaftConsensusServiceAsync().priorityVote(    // 通过代理类进行真正的 rpc 通信
                request, new PriorityVoteResponseCallback(this, peer, request));   // request 是传输的内容
    }

    private void qualificationConfirm(Peer peer) {
        LOG.info("begin qualification confirm request");
        RaftProto.QualificationConfirmRequest.Builder requestBuilder = RaftProto.QualificationConfirmRequest.newBuilder();
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
        RaftProto.QualificationConfirmRequest request = requestBuilder.build(); // 构建真正的 VoteRequest
        peer.getRaftConsensusServiceAsync().qualificationConfirm(    // 通过代理类进行真正的 rpc 通信
                request, new QualificationConfirmResponseCallback(this, peer, request));   // request 是传输的内容
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
                        if (votedFor == localServer.getServerId()) {    // 如果自己已经给自己投票了，那么就不能再给别人投票
                            voteGrantedNum += 1;    // 自己的得票数自增
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
        public void fail(Throwable e) { // 如果投票的响应结果为 false，直接进行日志打印
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

    // 向其它节点安装快照
    private boolean installSnapshot(Peer peer) {
        if (snapshot.getIsTakeSnapshot().get()) {   // 如果当前正在拍快照，那么就不能进行安装快照的操作
            LOG.info("already in take snapshot, please send install snapshot request later");
            return false;
        }
        if (!snapshot.getIsInstallSnapshot().compareAndSet(false, true)) {
            LOG.info("already in install snapshot");    // 更新正在安装快照的标志，标记为正在安装快照，避免重复操作
            return false;
        }
        // 开始发送安装快照请求到其它节点
        LOG.info("begin send install snapshot request to server={}", peer.getServer().getServerId());
        boolean isSuccess = true;   // 将快照目录下的文件全部包装为 SnapshotDataFile 通过 map 容器返回
        TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap = snapshot.openSnapshotDataFiles();
        LOG.info("total snapshot files={}", snapshotDataFileMap.keySet());  // 统计快照文件的数目
        try {
            boolean isLastRequest = false;
            String lastFileName = null;
            long lastOffset = 0;
            long lastLength = 0;
            while (!isLastRequest) {    // 构建 InstallSnapshotRequest，需要根据上一次操作的结果来决定本次请求的内容，比如数据长度，是否是第一或最后一个文件块
                RaftProto.InstallSnapshotRequest request
                        = buildInstallSnapshotRequest(snapshotDataFileMap, lastFileName, lastOffset, lastLength);
                if (request == null) {  // 如果构建的 InstallSnapshotRequest 为空
                    LOG.warn("snapshot request == null");
                    isSuccess = false;  // 标记本次的操作失败
                    break;
                }
                if (request.getIsLast()) {  // 判断是否是最后一个文件块了
                    isLastRequest = true;   // 标记是最后一个文件块了
                }   // 内容包括 fileName、offset、size、isFirst、isLast
                LOG.info("install snapshot request, fileName={}, offset={}, size={}, isFirst={}, isLast={}",
                        request.getFileName(), request.getOffset(), request.getData().toByteArray().length,
                        request.getIsFirst(), request.getIsLast());
                RaftProto.InstallSnapshotResponse response  // 发送 InstallSnapshotRequest
                        = peer.getRaftConsensusServiceAsync().installSnapshot(request);
                if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {  // 如果安装快照的操作成功
                    lastFileName = request.getFileName();   // 上一次操作的文件名
                    lastOffset = request.getOffset();   // 上一次操作的偏移量
                    lastLength = request.getData().size();  // 上一次操作的数据长度
                } else {    // 失败后直接 break
                    isSuccess = false;
                    break;
                }
            }

            if (isSuccess) {    // 如果安装快照成功
                long lastIncludedIndexInSnapshot;
                snapshot.getLock().lock();
                try {
                    lastIncludedIndexInSnapshot = snapshot.getMetaData().getLastIncludedIndex();
                } finally {
                    snapshot.getLock().unlock();
                }

                lock.lock();
                try {
                    peer.setNextIndex(lastIncludedIndexInSnapshot + 1); // 更新 peer 的 nextIndex 值
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            snapshot.closeSnapshotDataFiles(snapshotDataFileMap);   // 将打开的每个快照文件关闭
            snapshot.getIsInstallSnapshot().compareAndSet(true, false); // 更新当前的状态为不在安装快照
        }
        LOG.info("end send install snapshot request to server={}, success={}",
                peer.getServer().getServerId(), isSuccess);
        return isSuccess;
    }

    // 构建 InstallSnapshotRequest，需要根据上一次操作的结果来决定本次请求的内容，比如数据长度，是否是第一或最后一个文件块
    private RaftProto.InstallSnapshotRequest buildInstallSnapshotRequest(
            TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap,
            String lastFileName, long lastOffset, long lastLength) {
        RaftProto.InstallSnapshotRequest.Builder requestBuilder = RaftProto.InstallSnapshotRequest.newBuilder();

        snapshot.getLock().lock();  // 快照锁
        try {
            if (lastFileName == null) { // 上一条处理的快照文件为空
                lastFileName = snapshotDataFileMap.firstKey();  // 拿到第一个快照文件对应的 key
                lastOffset = 0; // 偏移量为 0
                lastLength = 0; // 上一次处理的文件长度也为 0
            }
            Snapshot.SnapshotDataFile lastFile = snapshotDataFileMap.get(lastFileName); // 拿到快照文件对应的 SnapshotDataFile
            long lastFileLength = lastFile.randomAccessFile.length();   // 数据的长度
            String currentFileName = lastFileName;  // 当前文件名
            long currentOffset = lastOffset + lastLength;   // 当前偏移量
            int currentDataSize = raftOptions.getMaxSnapshotBytesPerRequest();  // 每次请求的最大快照大小
            Snapshot.SnapshotDataFile currentDataFile = lastFile;   // 当前文件
            if (lastOffset + lastLength < lastFileLength) { // 如果上一次的偏移量加上一次的长度没有超过文件数据长度
                if (lastOffset + lastLength + raftOptions.getMaxSnapshotBytesPerRequest() > lastFileLength) {   // 如果本次操作的数据会超过文件的长度
                    currentDataSize = (int) (lastFileLength - (lastOffset + lastLength));   // 避免超过每次请求的数据上限
                }
            } else {    // 执行到这里，说明上一次的读取操作已经将当前文件读完了，需要转到下一个文件进行读取
                Map.Entry<String, Snapshot.SnapshotDataFile> currentEntry
                        = snapshotDataFileMap.higherEntry(lastFileName);    // 下一个快照文件
                if (currentEntry == null) { // 没有下一个文件了
                    LOG.warn("reach the last file={}", lastFileName);
                    return null;
                }
                currentDataFile = currentEntry.getValue();  // 下一个文件
                currentFileName = currentEntry.getKey();    // 下一个文件名
                currentOffset = 0;
                int currentFileLenght = (int) currentEntry.getValue().randomAccessFile.length();    // 下一个文件的长度
                if (currentFileLenght < raftOptions.getMaxSnapshotBytesPerRequest()) {  // 文件长度没有超过每次请求的最大快照长度
                    currentDataSize = currentFileLenght;    // 当前数据长度就是文件的数据长度
                }
            }
            byte[] currentData = new byte[currentDataSize]; // 构建存储数据的字节数组
            currentDataFile.randomAccessFile.seek(currentOffset);   // 偏移量
            currentDataFile.randomAccessFile.read(currentData); // 读取长度
            requestBuilder.setData(ByteString.copyFrom(currentData));   // 请求的数据
            requestBuilder.setFileName(currentFileName);    // 当前的文件名
            requestBuilder.setOffset(currentOffset);    // 当前的偏移量
            requestBuilder.setIsFirst(false);   // 初始化为不是第一个文件块
            if (currentFileName.equals(snapshotDataFileMap.lastKey())   // 如果正在处理最后一个快照文件且处理的数据已经超过了文件的长度
                    && currentOffset + currentDataSize >= currentDataFile.randomAccessFile.length()) {
                requestBuilder.setIsLast(true); // 标记这是最后一个文件块了
            } else {
                requestBuilder.setIsLast(false);    // 否则不是最后一个文件块
            }
            if (currentFileName.equals(snapshotDataFileMap.firstKey()) && currentOffset == 0) { // 如果正在处理第一个文件，且偏移量为 0
                requestBuilder.setIsFirst(true);    // 标记这是第一个文件块
                requestBuilder.setSnapshotMetaData(snapshot.getMetaData()); // 第一个文件块的 InstallSnapshotRequest 中需要包含快照的元数据
            } else {
                requestBuilder.setIsFirst(false);   // 否则标记不是第一个文件块
            }
        } catch (Exception ex) {
            LOG.warn("meet exception:", ex);
            return null;
        } finally {
            snapshot.getLock().unlock();
        }

        lock.lock();
        try {
            requestBuilder.setTerm(currentTerm);    // 设置任期信息
            requestBuilder.setServerId(localServer.getServerId());  // 设置自己的唯一标识符
        } finally {
            lock.unlock();
        }

        return requestBuilder.build();
    }

    /**
     * 判断 serverId 是否是资格表中优先级最高的节点
     *
     * @param serverId 节点 id
     * @return 如果是优先级最高的节点，返回 true，否则返回 false
     */
    public boolean isHighestPriority(int serverId) {
        for (Integer id : priorityTable) {
            if (qualificationTable.contains(id)) {
                return serverId == id;
            }
        }
        return false;
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

    public boolean isElectionLeader() {
        return electionLeader;
    }

    public void setElectionLeader(boolean electionLeader) {
        this.electionLeader = electionLeader;
    }

    public Set<Integer> getQualificationTable() {
        return qualificationTable;
    }
}
