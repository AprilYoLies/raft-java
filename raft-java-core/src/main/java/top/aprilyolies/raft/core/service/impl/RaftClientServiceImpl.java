package top.aprilyolies.raft.core.service.impl;

import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.aprilyolies.raft.core.Peer;
import top.aprilyolies.raft.core.RaftNode;
import top.aprilyolies.raft.core.RaftOptions;
import top.aprilyolies.raft.core.proto.RaftProto;
import top.aprilyolies.raft.core.service.RaftClientService;
import top.aprilyolies.raft.core.util.ConfigurationUtils;

import java.util.ArrayList;
import java.util.List;

public class RaftClientServiceImpl implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceImpl.class);
    private static final JsonFormat jsonFormat = new JsonFormat();

    private RaftNode raftNode;

    public RaftClientServiceImpl(RaftNode raftNode, RaftOptions raftOptions) {
        this.raftNode = raftNode;
    }

    @Override   // 获取 Leader 信息，如果自己不知道 Leader 信息，直接返回失败，否则构建好 Leader 信息后返回给请求的发送者
    public RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request) {
        LOG.info("receive getLeader request");
        RaftProto.GetLeaderResponse.Builder responseBuilder = RaftProto.GetLeaderResponse.newBuilder(); // GetLeaderResponse 构建器
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 默认状态是 success
        RaftProto.Endpoint.Builder endPointBuilder = RaftProto.Endpoint.newBuilder();   // Endpoint 构建器
        raftNode.getLock().lock();
        try {
            int leaderId = raftNode.getLeaderId();  // 获取 Leader 的信息
            if (leaderId == 0) {
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);    // 如果自己不知道 Leader 信息，设置响应为 fail
            } else if (leaderId == raftNode.getLocalServer().getServerId()) {   // 如果自己是 Leader 节点
                endPointBuilder.setHost(raftNode.getLocalServer().getEndpoint().getHost()); // 设置 ip
                endPointBuilder.setPort(raftNode.getLocalServer().getEndpoint().getPort()); // 设置端口号
            } else {
                RaftProto.Configuration configuration = raftNode.getConfiguration();    // 如果自己不是 Leader
                for (RaftProto.Server server : configuration.getServersList()) {    // 从配置列表中获取 leaderId 对应的 ip 和端口号
                    if (server.getServerId() == leaderId) {
                        endPointBuilder.setHost(server.getEndpoint().getHost());    // ip
                        endPointBuilder.setPort(server.getEndpoint().getPort());    // 端口号
                        break;
                    }
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }
        responseBuilder.setLeader(endPointBuilder.build()); // 设置 response 中的 endPoints 信息
        RaftProto.GetLeaderResponse response = responseBuilder.build(); // 构建响应
        LOG.info("getLeader response={}", jsonFormat.printToString(response));
        return response; // 返回
    }

    @Override
    public RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request) {
        RaftProto.GetConfigurationResponse.Builder responseBuilder
                = RaftProto.GetConfigurationResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 默认响应结果为 true
        raftNode.getLock().lock();
        try {
            RaftProto.Configuration configuration = raftNode.getConfiguration();    // 获取配置列表信息
            RaftProto.Server leader = ConfigurationUtils.getServer(configuration, raftNode.getLeaderId());  // 获取 leader 信息
            responseBuilder.setLeader(leader);  // 设置 leader 信息
            responseBuilder.addAllServers(configuration.getServersList());  // 添加全部节点的信息
        } finally {
            raftNode.getLock().unlock();
        }
        RaftProto.GetConfigurationResponse response = responseBuilder.build();  // 构建真正的 GetConfigurationResponse
        LOG.info("getConfiguration request={} response={}",
                jsonFormat.printToString(request), jsonFormat.printToString(response));

        return response;    // 返回
    }

    @Override
    public RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request) {
        RaftProto.AddPeersResponse.Builder responseBuilder = RaftProto.AddPeersResponse.newBuilder();   // AddPeersResponse 构建器
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);    // 默认的响应结果为 fail
        if (request.getServersCount() == 0  // 检查添加节点的数量，要求是 2 的倍数
                || request.getServersCount() % 2 != 0) {
            LOG.warn("added server's size can only multiple of 2");
            responseBuilder.setResMsg("added server's size can only multiple of 2");
            return responseBuilder.build();
        }
        for (RaftProto.Server server : request.getServersList()) {  // 检查是否添加的节点已经存在
            if (raftNode.getPeerMap().containsKey(server.getServerId())) {
                LOG.warn("already be added/adding to configuration");
                responseBuilder.setResMsg("already be added/adding to configuration");
                return responseBuilder.build();
            }
        }
        List<Peer> requestPeers = new ArrayList<>(request.getServersCount());   // 新 peer list
        for (RaftProto.Server server : request.getServersList()) {  // 便利要添加的 server
            final Peer peer = new Peer(server); // Server 封装为 Peer
            peer.setNextIndex(1);   // 新加入 peer 的 nextIndex 为 1
            requestPeers.add(peer); // 添加到 list 中
            raftNode.getPeerMap().putIfAbsent(server.getServerId(), peer);  // 将新的 peer 添加到 peer map 中
            raftNode.getExecutorService().submit(new Runnable() {
                @Override
                public void run() {
                    raftNode.appendEntries(peer, false);   // 向新加入的 peer 追加日志项
                }
            });
        }

        int catchUpNum = 0; // 记录新加入节点日志追赶完成的数量
        raftNode.getLock().lock();
        try {
            while (catchUpNum < requestPeers.size()) {  // 如果不是全部追赶上
                try {
                    raftNode.getCatchUpCondition().await(); // 等待日志追赶完成
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                catchUpNum = 0;
                for (Peer peer : requestPeers) {    // 检查日志追赶上的节点数量
                    if (peer.isCatchUp()) { // 如果节点的日志追赶上了
                        catchUpNum++;   // 数量加一
                    }
                }
                if (catchUpNum == requestPeers.size()) {
                    break;  // 如果新加入的节点的日志全部追赶上
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (catchUpNum == requestPeers.size()) {    // 如果新加入的节点的日志项全部追赶上了
            raftNode.getLock().lock();
            byte[] configurationData;
            RaftProto.Configuration newConfiguration;
            try {
                newConfiguration = RaftProto.Configuration.newBuilder(raftNode.getConfiguration())
                        .addAllServers(request.getServersList()).build();   // 构建 Configuration
                configurationData = newConfiguration.toByteArray(); // 将新的配置信息转换为字节数组
            } finally {
                raftNode.getLock().unlock();
            }   // 同步日志项，此时类型为 ENTRY_TYPE_CONFIGURATION
            boolean success = raftNode.replicate(configurationData, RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION);
            if (success) {  // 如果配置项同步成功
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 响应结果为 success
            }
        }
        if (responseBuilder.getResCode() != RaftProto.ResCode.RES_CODE_SUCCESS) {   // 如果同步配置项失败
            raftNode.getLock().lock();
            try {
                for (Peer peer : requestPeers) {    // 遍历新加入的 peers
                    peer.getRpcClient().stop(); // 关闭其 rpc 客户端
                    raftNode.getPeerMap().remove(peer.getServer().getServerId());   // 从本地 peerMap 中移除 peers
                }
            } finally {
                raftNode.getLock().unlock();
            }
        }

        RaftProto.AddPeersResponse response = responseBuilder.build();  // 构建真正的 AddPeersResponse
        LOG.info("addPeers request={} resCode={}",
                jsonFormat.printToString(request), response.getResCode());

        return response;    // 返回处理的结果
    }

    @Override
    public RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request) {
        RaftProto.RemovePeersResponse.Builder responseBuilder = RaftProto.RemovePeersResponse.newBuilder(); // RemovePeersResponse 构建器
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);    // 默认状态为 fail

        if (request.getServersCount() == 0  // 检查移除的节点的数量
                || request.getServersCount() % 2 != 0) {
            LOG.warn("removed server's size can only multiple of 2");
            responseBuilder.setResMsg("removed server's size can only multiple of 2");
            return responseBuilder.build();
        }

        // check request peers exist
        raftNode.getLock().lock();
        try {
            for (RaftProto.Server server : request.getServersList()) {
                if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), server.getServerId())) {
                    return responseBuilder.build(); // 如果集群中不存在要移除的节点，那么直接返回操作失败
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }

        raftNode.getLock().lock();
        RaftProto.Configuration newConfiguration;   // 新的配置项
        byte[] configurationData;
        try {
            newConfiguration = ConfigurationUtils.removeServers(    // 从 configuration 中移除全部的 servers
                    raftNode.getConfiguration(), request.getServersList());
            LOG.debug("newConfiguration={}", jsonFormat.printToString(newConfiguration));
            configurationData = newConfiguration.toByteArray(); // 将 Configuration 转换为字节数组
        } finally {
            raftNode.getLock().unlock();
        }   // 同步配置项信息
        boolean success = raftNode.replicate(configurationData, RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION);
        if (success) {
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS); // 如果同步成功
        }

        LOG.info("removePeers request={} resCode={}",
                jsonFormat.printToString(request), responseBuilder.getResCode());

        return responseBuilder.build(); // 返回处理的结果
    }

}
