package top.aprilyolies.raft.core.storage;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.aprilyolies.raft.core.proto.RaftProto;
import top.aprilyolies.raft.core.util.RaftFileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.TreeMap;

public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir;
    private String logDataDir;
    private int maxSegmentFileSize;
    private RaftProto.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
    // segment log占用的内存大小，用于判断是否需要做snapshot
    private volatile long totalSize;

    // 尝试加载段数据（本地），加载了本地的元数据，将日志段数据解析为 Segment，然后更新了元数据信息
    public SegmentedLog(String raftDataDir, int maxSegmentFileSize) {
        this.logDir = raftDataDir + File.separator + "log"; // ./data1/log
        this.logDataDir = logDir + File.separator + "data"; // ./data1/log/data
        this.maxSegmentFileSize = maxSegmentFileSize;   // 1024 * 1024
        File file = new File(logDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments(); // 尝试读本地文件，Segment 实例记录文件名（open 或者 start-end），交由 startLogIndexSegmentMap 保存
        for (Segment segment : startLogIndexSegmentMap.values()) {  // 段文件的加载
            this.loadSegmentData(segment);  // 将日志段段文件解析为 LogEntry list，然后将解析出来的 list 保存到 Segment 中，更新 totalSize 以及 startIndex 和 endIndex
        }

        metaData = this.readMetaData(); // 从元数据文件中读取数据，期间进行了 crc32 校验（currentTerm、votedFor、firstLogIndex）
        if (metaData == null) {
            if (startLogIndexSegmentMap.size() > 0) {   // 如果存在日志段文件，但是不存在元数据文件，不允许存在这种情况
                LOG.error("No readable metadata file but found segments in {}", logDir);
                throw new RuntimeException("No readable metadata file but found segments");
            }
            metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();  // 如果是初次启动，那么直接创建元数据文件，只需要指定 firstLogIndex
        }
    }

    // 获取 index 所在的 Segment，然后从中获取 index 对应的 LogEntry
    public RaftProto.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();    // 通过日志的元数据块获取第一条日志项的索引
        long lastLogIndex = getLastLogIndex();
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {  // 参数检查
            LOG.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        if (startLogIndexSegmentMap.size() == 0) {  // 如果当前没有日志段文件
            return null;
        }
        Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue(); // 小于等于 index 的最大 entry
        return segment.getEntry(index); // 从 Segment 中获取 index 对应的 LogEntry
    }

    // 获取 index 日志项的任期号
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
            } catch (IOException ex) {
                throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }

    // 根据快照的结果，删除 newFirstIndex 之前的过期日志项，然后更新日志的元数据信息
    public void truncatePrefix(long newFirstIndex) {    // 删除参数之前的全部日志项
        if (newFirstIndex <= getFirstLogIndex()) {  // 如果参数不满足删除的条件，直接返回
            return;
        }
        long oldFirstIndex = getFirstLogIndex();    // 首条日志项
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.firstEntry().getValue();  // 日志段和首条日志项索引的映射
            if (segment.isCanWrite()) { // 如果日志段索引可写，直接 break，就是找打了可以写入的日志段文件了
                break;
            }
            if (newFirstIndex > segment.getEndIndex()) {    // 如果该日志段文件是过期的
                File oldFile = new File(logDataDir + File.separator + segment.getFileName());   // 获取对应日志段的文件
                try {
                    RaftFileUtils.closeFile(segment.getRandomAccessFile()); // 关闭日志段文件
                    FileUtils.forceDelete(oldFile); // 删除过期的日志段文件
                    totalSize -= segment.getFileSize(); // 更新日志文件大小
                    startLogIndexSegmentMap.remove(segment.getStartIndex());    // 从日志段和首条日志项索引的映射中删除当前日志段的信息
                } catch (Exception ex2) {
                    LOG.warn("delete file exception:", ex2);
                }
            } else {
                break;  // 执行到这里说明找到了包含未过期的日志项的日志段
            }
        }
        long newActualFirstIndex;
        if (startLogIndexSegmentMap.size() == 0) {
            newActualFirstIndex = newFirstIndex;    // 更新新的日志中第一条日志项的索引
        } else {
            newActualFirstIndex = startLogIndexSegmentMap.firstKey();
        }
        updateMetaData(null, null, newActualFirstIndex);    // 更新元数据信息（日志的元数据信息）
        LOG.info("Truncating log from old first index {} to new first index {}",
                oldFirstIndex, newActualFirstIndex);
    }

    // 删除 newEndIndex 之后的日志项，包括 Segment 的 Record list 和日志段文件中内容，更新 Segment 相关的信息，重命名文件
    public void truncateSuffix(long newEndIndex) {
        if (newEndIndex >= getLastLogIndex()) { // 参数超过了范围，直接返回
            return;
        }
        LOG.info("Truncating log from old end index {} to new end index {}",
                getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {    // 如果开始索引和日志段文件映射 map 不为空
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();   // 先获取最后一个日志段文件
            try {
                if (newEndIndex == segment.getEndIndex()) { // 如果正巧最后一个日志段索引就是要砍掉的日志项索引
                    break;  // 那就不用砍掉了
                } else if (newEndIndex < segment.getStartIndex()) { // 如果待砍掉的日志项不在当前日志段文件
                    totalSize -= segment.getFileSize(); // 直接减去整个日志段文件的大小
                    // delete file
                    segment.getRandomAccessFile().close();  // 关闭当前日志段文件
                    String fullFileName = logDataDir + File.separator + segment.getFileName();  // 当前日志段文件名
                    FileUtils.forceDelete(new File(fullFileName));  // 删掉日志段文件
                    startLogIndexSegmentMap.remove(segment.getFileName());  // 从映射的 map 中移除该日志段
                } else if (newEndIndex < segment.getEndIndex()) {   // 如果待砍掉的日志项就在当前日志段文件中
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());  // 保留的长度
                    segment.setEndIndex(newEndIndex);   // 更新日志段的 endIndex
                    long newFileSize = segment.getEntries().get(i).offset;  // 待砍掉的日志项偏移量
                    totalSize -= (segment.getFileSize() - newFileSize); // 计算新的日志总大小
                    segment.setFileSize(newFileSize);   // 设置日志段文件的大小
                    segment.getEntries().removeAll( // 先从 Records list 中移除后边的全部日志项
                            segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel fileChannel = segment.getRandomAccessFile().getChannel();   // 打开文件 channel
                    fileChannel.truncate(segment.getFileSize());    // 从文件中砍掉剩下的内容
                    fileChannel.close();    // 关闭文件 channel
                    segment.getRandomAccessFile().close();  // 关闭文件
                    String oldFullFileName = logDataDir + File.separator + segment.getFileName();   // 之前文件的名字
                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());    // 新的名字
                    segment.setFileName(newFileName);   // 指定 segment 为新的名字
                    String newFullFileName = logDataDir + File.separator + segment.getFileName();
                    new File(oldFullFileName).renameTo(new File(newFullFileName));  // 重命名日志段文件
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));   // 将对应的 File 保存到 Segment 中
                }
            } catch (IOException ex) {
                LOG.warn("io exception, msg={}", ex.getMessage());
            }
        }
    }

    // 将日志段段文件解析为 LogEntry list，然后将解析出来的 list 保存到 Segment 中，更新 totalSize 以及 startIndex 和 endIndex
    public void loadSegmentData(Segment segment) {
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();  // 拿到 Segment 对应的日志文件
            long totalLength = segment.getFileSize();   // Segment 对应的日志文件的长度
            long offset = 0;
            while (offset < totalLength) {
                RaftProto.LogEntry entry = RaftFileUtils.readProtoFromFile( // 将文件中的数据读取并解析为 LogEntry（每一条记录都有一个 crc32 校验值和数据长度）
                        randomAccessFile, RaftProto.LogEntry.class);
                if (entry == null) {
                    throw new RuntimeException("read segment log failed");
                }
                Segment.Record record = new Segment.Record(offset, entry);  // Segment 中是通过 Record 来记录每一项日志项的
                segment.getEntries().add(record);   // 将对应的 Record 保存到 Segment 中
                offset = randomAccessFile.getFilePointer(); // 获取当前文件的偏移量
            }
            totalSize += totalLength;   // 更新总的日志段长度
        } catch (Exception ex) {
            LOG.error("read segment meet exception, msg={}", ex.getMessage());
            throw new RuntimeException("file not found");
        }

        int entrySize = segment.getEntries().size();    // 日志项的条数
        if (entrySize > 0) {
            segment.setStartIndex(segment.getEntries().get(0).entry.getIndex());    // Segment 记录本日志段的起始日志项索引
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).entry.getIndex());  // Segment 记录本日志段的截止日志项索引
        }
    }

    // 尝试读本地文件，Segment 实例记录文件名（open 或者 start-end），交由 startLogIndexSegmentMap 保存
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
                if (splitArray[0].equals("open")) { // 文件名的 open 表示可写
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.valueOf(splitArray[1]));
                    segment.setEndIndex(0);
                } else {
                    try {
                        segment.setCanWrite(false); // 如果文件名是 num-num，则分别表示 startIndex 和 endIndex
                        segment.setStartIndex(Long.parseLong(splitArray[0]));
                        segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException ex) {
                        LOG.warn("segment filename[{}] is not valid", fileName);
                        continue;
                    }
                }
                segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, fileName, "rw"));    // 打开 dir/fileName 对应的文件返回
                segment.setFileSize(segment.getRandomAccessFile().length());
                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } catch (IOException ioException) {
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
