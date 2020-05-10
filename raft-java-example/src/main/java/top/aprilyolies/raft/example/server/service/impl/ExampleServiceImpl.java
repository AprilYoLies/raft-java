package top.aprilyolies.raft.example.server.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.aprilyolies.raft.core.RaftNode;
import top.aprilyolies.raft.core.RaftOptions;
import top.aprilyolies.raft.core.proto.RaftProto;
import top.aprilyolies.raft.example.server.ExampleStateMachine;
import top.aprilyolies.raft.example.server.service.ExampleProto;
import top.aprilyolies.raft.example.server.service.ExampleService;

import java.util.concurrent.Semaphore;

public class ExampleServiceImpl implements ExampleService {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceImpl.class);
    private static JsonFormat jsonFormat = new JsonFormat();

    private RaftNode raftNode;
    private ExampleStateMachine stateMachine;
    private ExampleService exampleService;
    private Semaphore semaphore;

    public ExampleServiceImpl(RaftNode raftNode, ExampleStateMachine stateMachine, RaftOptions raftOptions) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.semaphore = new Semaphore(raftOptions.getConcurrentWindow());
    }

    @Override
    public ExampleProto.SetResponse set(ExampleProto.SetRequest request) {
        ExampleProto.SetResponse.Builder responseBuilder = ExampleProto.SetResponse.newBuilder();   // 构建对应的 SetResponse
        try {
            semaphore.acquire();
            // 如果自己不是leader，将写请求转发给leader
            if (raftNode.getLeaderId() <= 0) {  // 如果自己不知道 Leader 节点的信息，直接返回请求失败
                responseBuilder.setSuccess(false);
            } else if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) { // 如果自己不是 Leader
                LOG.info("receive set request, I'm not leader, dispatcher this request");
                if (this.exampleService == null) {
                    RpcClient rpcClient = raftNode.getPeerMap().get(raftNode.getLeaderId()).createClient();
                    exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);    // 得到对应的代理类
                }
                ExampleProto.SetResponse responseFromLeader = exampleService.set(request);  // 向 Leader 节点发起请求，得到请求结果
                responseBuilder.mergeFrom(responseFromLeader);  // 将两个结果合并
            } else {
                // 数据同步写入raft集群
                LOG.info("receive set request, I'm leader, process this request");
                byte[] data = request.toByteArray();    // 将请求序列化为字节数组
                boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_DATA);    // 进行真正的日志项的同步工作，将待写入的数据构建为 LogEntry，然后批量追加到日志文件中，最后等待日志项被应用，返回日志项应用的结果
                responseBuilder.setSuccess(success);    // 设置写入的结果
            }

            ExampleProto.SetResponse response = responseBuilder.build();    // 构建响应结果
            LOG.info("set request, request={}, response={}", jsonFormat.printToString(request),
                    jsonFormat.printToString(response));
            return response;    // 返回响应结果
        } catch (InterruptedException e) {
            responseBuilder.setSuccess(false);
            return responseBuilder.build();
        } finally {
            semaphore.release();
        }
    }

    @Override
    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        ExampleProto.GetResponse response = stateMachine.get(request);  // 从状态机中获取执行的结果
        LOG.info("get request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;    // 返回响应
    }

}
