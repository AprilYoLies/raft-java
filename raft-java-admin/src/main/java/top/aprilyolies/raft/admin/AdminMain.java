package top.aprilyolies.raft.admin;

import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang3.Validate;
import top.aprilyolies.raft.core.proto.RaftProto;
import top.aprilyolies.raft.core.service.RaftClientService;

import java.util.ArrayList;
import java.util.List;

public class AdminMain {
    private static final JsonFormat jsonFormat = new JsonFormat();

    public static void main(String[] args) {
        // parse args
        if (args.length < 3) {
            System.out.println("java -jar AdminMain servers cmd subCmd [args]");
            System.exit(1);
        }
        // servers format is like "10.1.1.1:8010:1,10.2.2.2:8011:2,10.3.3.3.3:8012:3"
        String servers = args[0];
        String cmd = args[1];
        String subCmd = args[2];
        Validate.isTrue(cmd.equals("conf"));    // 命令
        Validate.isTrue(subCmd.equals("get")    // 子命令校验
                || subCmd.equals("add")
                || subCmd.equals("del"));
        RaftClientService client = new RaftClientServiceProxy(servers); // 通过 clusterRaftClientService 获取到 Leader 信息，然后通过 Leader 的信息构建 leaderRaftClientService
        if (subCmd.equals("get")) {
            RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build(); // 构建 GetConfigurationRequest
            RaftProto.GetConfigurationResponse response = client.getConfiguration(request); // 通过 clusterRaftClientService 代理获取 GetConfigurationResponse
            if (response != null) {
                System.out.println(jsonFormat.printToString(response)); // 打印对应的结果
            } else {
                System.out.printf("response == null");
            }

        } else if (subCmd.equals("add")) {
            List<RaftProto.Server> serverList = parseServers(args[3]);
            RaftProto.AddPeersRequest request = RaftProto.AddPeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            RaftProto.AddPeersResponse response = client.addPeers(request);
            if (response != null) {
                System.out.println(response.getResCode());
            } else {
                System.out.printf("response == null");
            }
        } else if (subCmd.equals("del")) {
            List<RaftProto.Server> serverList = parseServers(args[3]);
            RaftProto.RemovePeersRequest request = RaftProto.RemovePeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            RaftProto.RemovePeersResponse response = client.removePeers(request);
            if (response != null) {
                System.out.println(response.getResCode());
            } else {
                System.out.printf("response == null");
            }
        }
        ((RaftClientServiceProxy) client).stop();
    }

    public static List<RaftProto.Server> parseServers(String serversString) {
        List<RaftProto.Server> serverList = new ArrayList<>();
        String[] splitArray1 = serversString.split(",");
        for (String addr : splitArray1) {
            String[] splitArray2 = addr.split(":");
            RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                    .setHost(splitArray2[0])
                    .setPort(Integer.parseInt(splitArray2[1])).build();
            RaftProto.Server server = RaftProto.Server.newBuilder()
                    .setEndpoint(endPoint)
                    .setServerId(Integer.parseInt(splitArray2[2])).build();
            serverList.add(server);
        }
        return serverList;
    }
}
