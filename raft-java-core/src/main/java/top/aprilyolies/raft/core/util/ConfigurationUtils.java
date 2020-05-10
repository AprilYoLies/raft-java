package top.aprilyolies.raft.core.util;


import top.aprilyolies.raft.core.proto.RaftProto;

import java.util.List;

public class ConfigurationUtils {

    // configuration不会太大，所以这里直接遍历了
    public static boolean containsServer(RaftProto.Configuration configuration, int serverId) {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return true;    // 如果配置项中包含自己，直接返回 true
            }
        }
        return false;
    }

    /**
     * 从 configuration 中移除全部的 servers
     *
     * @param configuration 原配置项
     * @param servers       待移除的节点
     * @return 剩余节点配置项
     */
    public static RaftProto.Configuration removeServers(
            RaftProto.Configuration configuration, List<RaftProto.Server> servers) {
        RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder(); // Configuration 构建器
        for (RaftProto.Server server : configuration.getServersList()) {    // 遍历原配置项的 servers
            boolean toBeRemoved = false;
            for (RaftProto.Server server1 : servers) {  // 遍历待移除的 servers
                if (server.getServerId() == server1.getServerId()) {    // 如果需要被移除
                    toBeRemoved = true;
                    break;
                }
            }
            if (!toBeRemoved) {
                confBuilder.addServers(server); // 添加不需要被移除的项
            }
        }
        return confBuilder.build();
    }

    public static RaftProto.Server getServer(RaftProto.Configuration configuration, int serverId) {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return server;
            }
        }
        return null;
    }

}
