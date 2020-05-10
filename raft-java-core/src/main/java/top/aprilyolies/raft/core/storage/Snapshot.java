package top.aprilyolies.raft.core.storage;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.aprilyolies.raft.core.proto.RaftProto;
import top.aprilyolies.raft.core.util.RaftFileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Snapshot {

    public class SnapshotDataFile {
        public String fileName;
        public RandomAccessFile randomAccessFile;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private String snapshotDir;
    private RaftProto.SnapshotMetaData metaData;
    // 表示是否正在安装snapshot，leader向follower安装，leader和follower同时处于installSnapshot状态
    private AtomicBoolean isInstallSnapshot = new AtomicBoolean(false);
    // 表示节点自己是否在对状态机做snapshot
    private AtomicBoolean isTakeSnapshot = new AtomicBoolean(false);
    private Lock lock = new ReentrantLock();

    public Snapshot(String raftDataDir) {   // 创建快照类，主要是创建了快照对应的目录
        this.snapshotDir = raftDataDir + File.separator + "snapshot";   // 快照目录 ./data1/snapshot
        String snapshotDataDir = snapshotDir + File.separator + "data"; // ./data1/snapshot/data
        File file = new File(snapshotDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    // 尝试从本地获取快照元数据，没有的话就构建一个
    public void reload() {
        metaData = this.readMetaData(); // 尝试读取本地快照的元数据
        if (metaData == null) {
            metaData = RaftProto.SnapshotMetaData.newBuilder().build(); // 本地没有快照元数据，那么就自己构建一个
        }
    }

    /**
     * 打开snapshot data目录下的文件，
     * 如果是软链接，需要打开实际文件句柄
     *
     * @return 文件名以及文件句柄map
     */ // 将快照目录下的文件全部包装为 SnapshotDataFile 通过 map 容器返回
    public TreeMap<String, SnapshotDataFile> openSnapshotDataFiles() {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();    // 快照数据文件 map
        String snapshotDataDir = snapshotDir + File.separator + "data"; // 快照数据文件夹
        try {
            Path snapshotDataPath = FileSystems.getDefault().getPath(snapshotDataDir);  // 快照数据路径
            snapshotDataPath = snapshotDataPath.toRealPath();
            snapshotDataDir = snapshotDataPath.toString();  // 真实的快照数据路径
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(snapshotDataDir, snapshotDataDir); // 获取目录下的全部文件，排序后返回
            for (String fileName : fileNames) {
                RandomAccessFile randomAccessFile = RaftFileUtils.openFile(snapshotDataDir, fileName, "r"); // 打开文件
                SnapshotDataFile snapshotFile = new SnapshotDataFile(); // 创建 SnapshotDataFile
                snapshotFile.fileName = fileName;   // 保存文件名到 SnapshotDataFile
                snapshotFile.randomAccessFile = randomAccessFile;   // 保存 file 到 SnapshotDataFile
                snapshotDataFileMap.put(fileName, snapshotFile);    // 将 SnapshotDataFile 保存到 map 中
            }
        } catch (IOException ex) {
            LOG.warn("readSnapshotDataFiles exception:", ex);
            throw new RuntimeException(ex);
        }
        return snapshotDataFileMap;
    }

    // 将打开的每个快照文件关闭
    public void closeSnapshotDataFiles(TreeMap<String, SnapshotDataFile> snapshotDataFileMap) {
        for (Map.Entry<String, SnapshotDataFile> entry : snapshotDataFileMap.entrySet()) {
            try {
                entry.getValue().randomAccessFile.close();
            } catch (IOException ex) {
                LOG.warn("close snapshot files exception:", ex);
            }
        }
    }

    // 尝试读取本地快照的元数据
    public RaftProto.SnapshotMetaData readMetaData() {
        String fileName = snapshotDir + File.separator + "metadata";    // 快照元数据对应的文件夹
        File file = new File(fileName); // 快照元数据文件夹对应的 File
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            RaftProto.SnapshotMetaData metadata = RaftFileUtils.readProtoFromFile(
                    randomAccessFile, RaftProto.SnapshotMetaData.class);    // 从元数据文件中读取数据，期间进行了 crc32 校验，然后将数据解析成为对应的类型返回
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    // 即将最后包含的日志项索引、日志项任期、集群节点信息保存到快照元数据文件中
    public void updateMetaData(String dir,
                               Long lastIncludedIndex,
                               Long lastIncludedTerm,
                               RaftProto.Configuration configuration) {
        RaftProto.SnapshotMetaData snapshotMetaData = RaftProto.SnapshotMetaData.newBuilder()   // 构建 SnapshotMetaData
                .setLastIncludedIndex(lastIncludedIndex)    // 最后包含的日志项索引
                .setLastIncludedTerm(lastIncludedTerm)  // 最后包含的日志项的任期号
                .setConfiguration(configuration).build();   // 配置信息（里边就是集群节点的信息）
        String snapshotMetaFile = dir + File.separator + "metadata";    // 快照元文件
        RandomAccessFile randomAccessFile = null;
        try {
            File dirFile = new File(dir);   // 创建日志快照元文件的文件夹
            if (!dirFile.exists()) {
                dirFile.mkdirs();
            }

            File file = new File(snapshotMetaFile); // 在快照文件夹下创建快照元文件
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
            file.createNewFile();
            randomAccessFile = new RandomAccessFile(file, "rw");
            RaftFileUtils.writeProtoToFile(randomAccessFile, snapshotMetaData); // 将快照元数据信息写入快照元数据文件
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", snapshotMetaFile);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
        }
    }

    public RaftProto.SnapshotMetaData getMetaData() {
        return metaData;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public AtomicBoolean getIsInstallSnapshot() {
        return isInstallSnapshot;
    }

    public AtomicBoolean getIsTakeSnapshot() {
        return isTakeSnapshot;
    }

    public Lock getLock() {
        return lock;
    }
}
