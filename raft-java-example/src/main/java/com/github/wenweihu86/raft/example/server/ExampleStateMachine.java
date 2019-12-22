package com.github.wenweihu86.raft.example.server;

import com.github.wenweihu86.raft.StateMachine;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ExampleStateMachine implements StateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleStateMachine.class);

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private String raftDataDir;

    public ExampleStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }

    @Override   // 创建 RocksDB 的快照，然后将结果保存到快照目录
    public void writeSnapshot(String snapshotDir) {
        Checkpoint checkpoint = Checkpoint.create(db);  // 创建检查点
        try {
            checkpoint.createCheckpoint(snapshotDir);   // 将检查点之前的数据保存到快照数据文件夹
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.warn("writeSnapshot meet exception, dir={}, msg={}",
                    snapshotDir, ex.getMessage());
        }
    }

    @Override   // 如果 rocksdb 文件夹存在，删除，然后将快照的文件夹拷贝到 rocksdb 中，最后构建 RocksDB，打开位置为 rocksdb 目录
    public void readSnapshot(String snapshotDir) {
        try {
            // copy snapshot dir to data dir
            if (db != null) {   // 直接关闭 RocksDB（快照就包含了全部的执行结果）
                db.close();
            }
            String dataDir = raftDataDir + File.separator + "rocksdb_data"; // RocksDB 的数据目录
            File dataFile = new File(dataDir);  // /Users/eva/IdeaProjects/raft-java/raft-java-example/data/rocksdb_data    // RocksDB 的数据目录对应的 File
            if (dataFile.exists()) {    // 如果 RocksDB 的数据目录已存在，删除存在的数据目录
                FileUtils.deleteDirectory(dataFile);
            }
            File snapshotFile = new File(snapshotDir);  // 快照文件对应的 File
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile, dataFile);    // 将快照 data 拷贝到 rocksdb 中
            }
            // open rocksdb data dir
            Options options = new Options();
            options.setCreateIfMissing(true);
            db = RocksDB.open(options, dataDir);    // RocksDB 直接打开快照就是执行后的结果
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }

    @Override
    public void apply(byte[] dataBytes) {   // 仅仅是将 key value 保存至 RocksDB 中
        try {
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }
    // 仅仅是根据 key 值获取 RocksDB 中的值，通过 GetResponse 返回
    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        try {
            ExampleProto.GetResponse.Builder responseBuilder = ExampleProto.GetResponse.newBuilder();
            byte[] keyBytes = request.getKey().getBytes();
            byte[] valueBytes = db.get(keyBytes);
            if (valueBytes != null) {
                String value = new String(valueBytes);
                responseBuilder.setValue(value);
            }
            ExampleProto.GetResponse response = responseBuilder.build();
            return response;
        } catch (RocksDBException ex) {
            LOG.warn("read rockdb error, msg={}", ex.getMessage());
            return null;
        }
    }

}
