package top.aprilyolies.raft.admin;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.aprilyolies.raft.core.proto.RaftProto;
import top.aprilyolies.raft.core.service.RaftClientService;

import java.util.List;

public class RaftClientServiceProxy implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceProxy.class);
    private static final JsonFormat jsonFormat = new JsonFormat();

    private List<RaftProto.Server> cluster;
    private RpcClient clusterRPCClient;
    private RaftClientService clusterRaftClientService;

    private RaftProto.Server leader;
    private RpcClient leaderRPCClient;
    private RaftClientService leaderRaftClientService;

    private RpcClientOptions rpcClientOptions = new RpcClientOptions();

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public RaftClientServiceProxy(String ipPorts) {
        rpcClientOptions.setConnectTimeoutMillis(1000); // 1s
        rpcClientOptions.setReadTimeoutMillis(3600000); // 1hour
        rpcClientOptions.setWriteTimeoutMillis(1000); // 1s
        clusterRPCClient = new RpcClient(ipPorts, rpcClientOptions);    // 构建 RpcClient
        clusterRaftClientService = BrpcProxy.getProxy(clusterRPCClient, RaftClientService.class);   // 创建对应的服务代理类
        updateConfiguration();  // 通过 clusterRaftClientService 获取到 Leader 信息，然后通过 Leader 的信息构建 leaderRaftClientService
    }

    @Override
    public RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request) {
        return clusterRaftClientService.getLeader(request);
    }

    @Override
    public RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request) {
        return clusterRaftClientService.getConfiguration(request);  // 通过 clusterRaftClientService 代理获取 GetConfigurationResponse
    }

    @Override
    public RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request) {
        RaftProto.AddPeersResponse response = leaderRaftClientService.addPeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.addPeers(request);
        }
        return response;
    }

    @Override
    public RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request) {
        RaftProto.RemovePeersResponse response = leaderRaftClientService.removePeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.removePeers(request);
        }
        return response;
    }

    public void stop() {
        if (leaderRPCClient != null) {
            leaderRPCClient.stop();
        }
        if (clusterRPCClient != null) {
            clusterRPCClient.stop();
        }
    }

    // 通过 clusterRaftClientService 获取到 Leader 信息，然后通过 Leader 的信息构建 leaderRaftClientService
    private boolean updateConfiguration() {
        RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build(); // 构建 GetConfigurationRequest
        RaftProto.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);   // 通过代理类发送 GetConfigurationRequest
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {  // 如果响应成功
            if (leaderRPCClient != null) {
                leaderRPCClient.stop();
            }
            leader = response.getLeader();  // Leader 信息
            leaderRPCClient = new RpcClient(convertEndPoint(leader.getEndpoint()), rpcClientOptions);   // 构建 RpcClient
            leaderRaftClientService = BrpcProxy.getProxy(leaderRPCClient, RaftClientService.class); // 构建 RaftClientService 服务对应的代理类
            return true;
        }
        return false;
    }

    private Endpoint convertEndPoint(RaftProto.Endpoint endPoint) {
        return new Endpoint(endPoint.getHost(), endPoint.getPort());
    }

}
