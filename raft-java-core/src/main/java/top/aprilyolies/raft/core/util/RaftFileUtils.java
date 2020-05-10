package top.aprilyolies.raft.core.util;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.zip.CRC32;

@SuppressWarnings("unchecked")
public class RaftFileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RaftFileUtils.class);

    // 列出 /Users/eva/IdeaProjects/raft-java/raft-java-example/data/log/data 下边的全部文件
    public static List<String> getSortedFilesInDirectory(   // 获取目录下的全部文件，排序后返回
                                                            String dirName, String rootDirName) throws IOException {
        List<String> fileList = new ArrayList<>();
        File rootDir = new File(rootDirName);   // 根文件夹
        File dir = new File(dirName);   // 文件夹名
        if (!rootDir.isDirectory() || !dir.isDirectory()) {
            return fileList;    // 如果两个参数都不是目录，直接返回空 list
        }
        String rootPath = rootDir.getCanonicalPath();   // 权威路径
        if (!rootPath.endsWith("/")) {
            rootPath = rootPath + "/";  // 补上路径分隔符
        }
        File[] files = dir.listFiles(); // 列出下边的全部文件
        for (File file : files) {
            if (file.isDirectory()) {   // 递归列出下边的文件
                fileList.addAll(getSortedFilesInDirectory(file.getCanonicalPath(), rootPath));
            } else {    // 将文件添加到文件 list 中
                fileList.add(file.getCanonicalPath().substring(rootPath.length()));
            }
        }
        Collections.sort(fileList);
        return fileList;
    }

    // 打开 dir/fileName 对应的文件返回
    public static RandomAccessFile openFile(String dir, String fileName, String mode) {
        try {
            String fullFileName = dir + File.separator + fileName;
            File file = new File(fullFileName);
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException ex) {
            LOG.warn("file not fount, file={}", fileName);
            throw new RuntimeException("file not found, file=" + fileName);
        }
    }

    public static void closeFile(RandomAccessFile randomAccessFile) {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ex) {
            LOG.warn("close file error, msg={}", ex.getMessage());
        }
    }

    public static void closeFile(FileInputStream inputStream) {
        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException ex) {
            LOG.warn("close file error, msg={}", ex.getMessage());
        }
    }

    public static void closeFile(FileOutputStream outputStream) {
        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (IOException ex) {
            LOG.warn("close file error , msg={}", ex.getMessage());
        }
    }

    // 从元数据文件中读取数据，期间进行了 crc32 校验，然后将数据解析成为对应的类型返回
    public static <T extends Message> T readProtoFromFile(RandomAccessFile raf, Class<T> clazz) {
        try {
            long crc32FromFile = raf.readLong();    // crc32 校验值
            int dataLen = raf.readInt();    // 数据长度
            int hasReadLen = (Long.SIZE + Integer.SIZE) / Byte.SIZE;    // 已读数据长度
            if (raf.length() - hasReadLen < dataLen) {  // 如果后边的数据不够长度
                LOG.warn("file remainLength < dataLen");    // 读取失败
                return null;
            }
            byte[] data = new byte[dataLen];    // 存储数据的字节数组
            int readLen = raf.read(data);   // 读取真正的数据
            if (readLen != dataLen) {   // 读取的长度不等于记录长度
                LOG.warn("readLen != dataLen");
                return null;
            }
            long crc32FromData = getCRC32(data);    // 获取已读数据的校验值
            if (crc32FromFile != crc32FromData) {   // 校验出现问题
                LOG.warn("crc32 check failed");
                return null;
            }
            Method method = clazz.getMethod("parseFrom", byte[].class); // 解析成为对应的类型
            T message = (T) method.invoke(clazz, data);
            return message;
        } catch (Exception ex) {
            LOG.warn("readProtoFromFile meet exception, {}", ex.getMessage());
            return null;
        }
    }

    // 将数据段按照一定的协议写入文件中
    public static <T extends Message> void writeProtoToFile(RandomAccessFile raf, T message) {
        byte[] messageBytes = message.toByteArray();    // 消息转换为字节数组
        long crc32 = getCRC32(messageBytes);    // 得到 crc32 校验结果
        try {
            raf.writeLong(crc32);   // 先写校验结果
            raf.writeInt(messageBytes.length);  // 再写数据长度
            raf.write(messageBytes);    // 最后写真正的数据内容
        } catch (IOException ex) {
            LOG.warn("write proto to file error, msg={}", ex.getMessage());
            throw new RuntimeException("write proto to file error");
        }
    }

    public static long getCRC32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

}
