package top.aprilyolies.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import top.aprilyolies.raft.example.server.service.ExampleProto;
import top.aprilyolies.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;

public class ClientMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.printf("Usage: ./run_client.sh CLUSTER KEY [VALUE]\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        String key = args[1];
        String value = null;
        if (args.length > 2) {
            value = args[2];
        }

        // init rpc client
        RpcClient rpcClient = new RpcClient(ipPorts);   // 构建 rpc 客户端
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);    // 获取对应的代理类
        final JsonFormat jsonFormat = new JsonFormat();

        // set
        for (int i = 0; i < 20; i++) {
            if (value != null) {
                ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()   // 构建对应的GET请求消息
                        .setKey(key + i).setValue(value + i).build();
                ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
                System.out.printf("set request, key=%s value=%s response=%s\n",
                        key + i, value + i, jsonFormat.printToString(setResponse));
            } else {
                // get
                ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()   // 构建对应的PUT请求消息
                        .setKey(key + i).build();
                ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
                System.out.printf("get request, key=%s, response=%s\n",
                        key + i, jsonFormat.printToString(getResponse));
            }
        }

        rpcClient.stop();
    }
}
