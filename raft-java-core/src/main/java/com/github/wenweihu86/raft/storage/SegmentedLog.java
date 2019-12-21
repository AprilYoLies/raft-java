package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.github.wenweihu86.raft.proto.RaftProto;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir;
    private String logDataDir;
    private int maxSegmentFileSize;
    private RaftProto.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
    // segment log占用的内存大小，用于判断是否需要做snapshot
    private volatile long totalSize;
    // 尝试加载段数据（本地），加载了本地的元数据
    public SegmentedLog(String raftDataDir, int maxSegmentFileSize) {
        this.logDir = raftDataDir + File.separator + "log";
        this.logDataDir = logDir + File.separator + "data";
        this.maxSegmentFileSize = maxSegmentFileSize;
        File file = new File(logDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments(); // 尝试读本地文件
        for (Segment segment : startLogIndexSegmentMap.values()) {  // 段文件的加载
            this.loadSegmentData(segment);
        }

        metaData = this.readMetaData(); // 从元数据文件中读取数据，期间进行了 crc32 校验
        if (metaData == null) {
            if (startLogIndexSegmentMap.size() > 0) {
                LOG.error("No readable metadata file but found segments in {}", logDir);
                throw new RuntimeException("No readable metadata file but found segments");
            }
            metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();
        }
    }

    public RaftProto.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();    // 通过日志的元数据块获取第一条日志项的索引
        long lastLogIndex = getLastLogIndex();
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            LOG.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        if (startLogIndexSegmentMap.size() == 0) {
            return null;
        }
        Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue();
        return segment.getEntry(index);
    }

    public long getEntryTerm(long index) {
        RaftProto.LogEntry entry = getEntry(index);
        if (entry == null) {
            return 0;
        }
        return entry.getTerm();
    }
    // 通过日志的元数据块获取第一条日志项的索引
    public long getFirstLogIndex() {
        return metaData.getFirstLogIndex();
    }
    // 获取最后一个日志项的索引（最新日志项的索引）
    public long getLastLogIndex() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startLogIndexSegmentMap.size() == 0) {
            return getFirstLogIndex() - 1;  // 如果暂时没有日志块，那就是元数据日志的第一条日志项的索引值减一
        }
        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();   // 获取最后一个日志块的最后一个日志项
        return lastSegment.getEndIndex();
    }
    // 判断是否需要新的日志段文件，然后将日志项写入到日志段文件中，更新对应的记录信息，返回写入后新的 LastLogIndex
    public long append(List<RaftProto.LogEntry> entries) {
        long newLastLogIndex = this.getLastLogIndex();  // 最后一条日志项的索引
        for (RaftProto.LogEntry entry : entries) {
            newLastLogIndex++;  // 新的写入位置
            int entrySize = entry.getSerializedSize();  // 序列化后的大小
            int segmentSize = startLogIndexSegmentMap.size();   // 日志段的数量
            boolean isNeedNewSegmentFile = false;   // 是否需要新的日志段文件的标志
            try {
                if (segmentSize == 0) { // 如果日志段文件的数量为 0，那么一定需要新的日志段文件
                    isNeedNewSegmentFile = true;
                } else {
                    Segment segment = startLogIndexSegmentMap.lastEntry().getValue();   // 拿到最后一个日志段文件
                    if (!segment.isCanWrite()) {    // 如果新的日志段文件不可写（达到上限）
                        isNeedNewSegmentFile = true;    // 同样需要新的日志段文件
                    } else if (segment.getFileSize() + entrySize >= maxSegmentFileSize) {   // 如果新写入的数据会导致超过上限
                        isNeedNewSegmentFile = true;    // 同样需要新的日志段文件
                        // 最后一个segment的文件close并改名
                        segment.getRandomAccessFile().close();  // 关闭最后一个日志段文件
                        segment.setCanWrite(false); // 标记它不可写了
                        String newFileName = String.format("%020d-%020d",   // 日志段文件的命名是按照起始索引-截止索引命名
                                segment.getStartIndex(), segment.getEndIndex());
                        String newFullFileName = logDataDir + File.separator + newFileName; // 日志文件的全限定名
                        File newFile = new File(newFullFileName);
                        String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                        File oldFile = new File(oldFullFileName);
                        FileUtils.moveFile(oldFile, newFile);   // 将日志段文件重命名
                        segment.setFileName(newFileName);   // 重新打开重命名后的文件，设置为只读
                        segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newFileName, "r"));
                    }
                }
                Segment newSegment;
                // 新建segment文件
                if (isNeedNewSegmentFile) {
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d", newLastLogIndex);  // 新的日志段文件名字
                    String newFullFileName = logDataDir + File.separator + newSegmentFileName;  // 新的日志段文件全路径
                    File newSegmentFile = new File(newFullFileName);    // 新的 file
                    if (!newSegmentFile.exists()) {
                        newSegmentFile.createNewFile(); // 不存在的话，就创建新的日志段文件
                    }
                    Segment segment = new Segment();    // 创建一个段信息并初始化
                    segment.setCanWrite(true);  // 可写
                    segment.setStartIndex(newLastLogIndex); // 起始索引
                    segment.setEndIndex(0); // 截止索引未知，初始化为 0
                    segment.setFileName(newSegmentFileName);    // 新的段文件名
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));  // 打开新的段文件
                    newSegment = segment;
                } else {
                    newSegment = startLogIndexSegmentMap.lastEntry().getValue();    // 如果不需要新的日志段文件，就直接使用当前最后一个日志项文件
                }
                // 写proto到segment中
                if (entry.getIndex() == 0) {    // 如果未指定日志项的索引，现在指定日志项索引
                    entry = RaftProto.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).build();
                }
                newSegment.setEndIndex(entry.getIndex());   // 更新段的截止索引为现在写入的日志项索引
                newSegment.getEntries().add(new Segment.Record( // Segment 中存入的是 Record 信息
                        newSegment.getRandomAccessFile().getFilePointer(), entry));
                RaftFileUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);    // 将数据段按照一定的协议写入文件中
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());  // 更新日志段文件大小
                if (!startLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) { // 将每个日志段的首日志项索引和日志段文件映射到 map 中
                    startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                }
                totalSize += entrySize; // 总的日志段大小
            }  catch (IOException ex) {
                throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }

    public void truncatePrefix(long newFirstIndex) {
        if (newFirstIndex <= getFirstLogIndex()) {
            return;
        }
        long oldFirstIndex = getFirstLogIndex();
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite()) {
                break;
            }
            if (newFirstIndex > segment.getEndIndex()) {
                File oldFile = new File(logDataDir + File.separator + segment.getFileName());
                try {
                    RaftFileUtils.closeFile(segment.getRandomAccessFile());
                    FileUtils.forceDelete(oldFile);
                    totalSize -= segment.getFileSize();
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } catch (Exception ex2) {
                    LOG.warn("delete file exception:", ex2);
                }
            } else {
                break;
            }
        }
        long newActualFirstIndex;
        if (startLogIndexSegmentMap.size() == 0) {
            newActualFirstIndex = newFirstIndex;
        } else {
            newActualFirstIndex = startLogIndexSegmentMap.firstKey();
        }
        updateMetaData(null, null, newActualFirstIndex);
        LOG.info("Truncating log from old first index {} to new first index {}",
                oldFirstIndex, newActualFirstIndex);
    }

    public void truncateSuffix(long newEndIndex) {
        if (newEndIndex >= getLastLogIndex()) {
            return;
        }
        LOG.info("Truncating log from old end index {} to new end index {}",
                getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            try {
                if (newEndIndex == segment.getEndIndex()) {
                    break;
                } else if (newEndIndex < segment.getStartIndex()) {
                    totalSize -= segment.getFileSize();
                    // delete file
                    segment.getRandomAccessFile().close();
                    String fullFileName = logDataDir + File.separator + segment.getFileName();
                    FileUtils.forceDelete(new File(fullFileName));
                    startLogIndexSegmentMap.remove(segment.getFileName());
                } else if (newEndIndex < segment.getEndIndex()) {
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                    segment.setEndIndex(newEndIndex);
                    long newFileSize = segment.getEntries().get(i).offset;
                    totalSize -= (segment.getFileSize() - newFileSize);
                    segment.setFileSize(newFileSize);
                    segment.getEntries().removeAll(
                            segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel fileChannel = segment.getRandomAccessFile().getChannel();
                    fileChannel.truncate(segment.getFileSize());
                    fileChannel.close();
                    segment.getRandomAccessFile().close();
                    String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());
                    segment.setFileName(newFileName);
                    String newFullFileName = logDataDir + File.separator + segment.getFileName();
                    new File(oldFullFileName).renameTo(new File(newFullFileName));
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));
                }
            } catch (IOException ex) {
                LOG.warn("io exception, msg={}", ex.getMessage());
            }
        }
    }

    public void loadSegmentData(Segment segment) {
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0;
            while (offset < totalLength) {
                RaftProto.LogEntry entry = RaftFileUtils.readProtoFromFile(
                        randomAccessFile, RaftProto.LogEntry.class);
                if (entry == null) {
                    throw new RuntimeException("read segment log failed");
                }
                Segment.Record record = new Segment.Record(offset, entry);
                segment.getEntries().add(record);
                offset = randomAccessFile.getFilePointer();
            }
            totalSize += totalLength;
        } catch (Exception ex) {
            LOG.error("read segment meet exception, msg={}", ex.getMessage());
            throw new RuntimeException("file not found");
        }

        int entrySize = segment.getEntries().size();
        if (entrySize > 0) {
            segment.setStartIndex(segment.getEntries().get(0).entry.getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).entry.getIndex());
        }
    }
    // 尝试读本地文件
    public void readSegments() {
        try {   // 列出 /Users/eva/IdeaProjects/raft-java/raft-java-example/data/log/data 下边的全部文件
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(logDataDir, logDataDir);
            for (String fileName : fileNames) {
                String[] splitArray = fileName.split("-");
                if (splitArray.length != 2) {
                    LOG.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }
                Segment segment = new Segment();
                segment.setFileName(fileName);
                if (splitArray[0].equals("open")) {
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.valueOf(splitArray[1]));
                    segment.setEndIndex(0);
                } else {
                    try {
                        segment.setCanWrite(false);
                        segment.setStartIndex(Long.parseLong(splitArray[0]));
                        segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException ex) {
                        LOG.warn("segment filename[{}] is not valid", fileName);
                        continue;
                    }
                }
                segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, fileName, "rw"));
                segment.setFileSize(segment.getRandomAccessFile().length());
                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } catch(IOException ioException){
            LOG.warn("readSegments exception:", ioException);
            throw new RuntimeException("open segment file error");
        }
    }
    // 从元数据文件中读取数据，期间进行了 crc32 校验
    public RaftProto.LogMetaData readMetaData() {
        String fileName = logDir + File.separator + "metadata"; // /Users/eva/IdeaProjects/raft-java/raft-java-example/data/log/metadata
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            RaftProto.LogMetaData metadata = RaftFileUtils.readProtoFromFile(   // 从元数据文件中读取数据，期间进行了 crc32 校验
                    randomAccessFile, RaftProto.LogMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }
    // 创建元数据文件，将元数据信息写入元数据文件中 currentTerm=18, votedFor=2, firstLogIndex=1
    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex) {
        RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData);    // 构建新的 LogMetaData
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);    // 当前任期
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);  // 为谁投票
        }
        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);    // 第一条日志项的索引
        }
        this.metaData = builder.build();    // 构建真正的 LogMetaData

        String fileName = logDir + File.separator + "metadata"; // 元数据文件名
        File file = new File(fileName); // 创建元数据文件
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            RaftFileUtils.writeProtoToFile(randomAccessFile, metaData); // 将元数据信息写入元数据文件中
            LOG.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
                    metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

    public RaftProto.LogMetaData getMetaData() {
        return metaData;
    }

    public long getTotalSize() {
        return totalSize;
    }

}
