package top.aprilyolies.analyze;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class RandomElectionAnylyze {
    public static void main(String[] args) throws Exception {
        int electionTimes = 30;    // 选举次数
        String logFile = "nohup.out";
        int nodeNum = 3;
        for (int i = 0; i < 3; i++) {
            int curNodeNum = nodeNum + 4 * i;
            for (int j = 2; j <= 3; j++) {
                int randomRange = j * 50;
                String dirName = "raft-java-analyze/random_election/" + "node_num_" + curNodeNum + "_random_range_" + randomRange + "/";
                File rootDir = new File(dirName);
                File[] files = rootDir.listFiles(File::isDirectory);

                Map<String, Integer> counts = new HashMap<>();
                long totalCosts = 0;
                int countTimes = 0;
                int once = 0;
                int twice = 0;
                int threeTimes = 0;
                int more = 0;

                for (File file : files) {
                    String nodeName = file.getName();
                    System.out.println("processing " + dirName + nodeName);
                    File[] logFiles = file.listFiles(file1 -> logFile.equals(file1.getName()));
                    if (logFiles == null || logFiles.length != 1) {
                        System.out.println("log analyze failed.");
                        return;
                    }
                    counts.put(nodeName, 0);

                    File logFile1 = logFiles[0];
                    RandomAccessFile raf = new RandomAccessFile(logFile1, "r");
                    String line = raf.readLine();
                    while (line != null) {
                        int idx = line.lastIndexOf("costs");
                        if (idx > 0) {
                            String costsStr = line.substring(idx);
                            Integer costs = Integer.parseInt(costsStr.substring(6, costsStr.lastIndexOf("ms")));
                            if (costs < 3000) {
                                line = raf.readLine();
                                continue;
                            } else if (costs < 6000) {
                                once++;
                            } else if (costs < 9000) {
                                twice++;
                            } else if (costs < 12000) {
                                threeTimes++;
                            } else {
                                more++;
                            }
                            totalCosts += costs;
                            counts.computeIfPresent(nodeName, (nodeName1, count) -> count += 1);
                            countTimes += 1;
                        }
                        line = raf.readLine();
                    }
                }
                File file = new File(dirName + "results.txt");
                if (file.exists()) {
                    file.delete();
                }
                if (file.createNewFile()) {
                    RandomAccessFile resultFile = new RandomAccessFile(file, "rw");
                    resultFile.writeChars(dirName);
                    resultFile.writeChars("\n");
                    for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                        String line = entry.getKey() + " become leader " + entry.getValue() + " times";
                        resultFile.writeChars(line);
                        resultFile.writeChars("\n");
                    }
                    String line = "election leader " + electionTimes + " times, success " + countTimes + " times, " +
                            "total costs " + totalCosts + "ms, average " + (totalCosts / countTimes) + "ms per election";
                    resultFile.writeChars(line);
                    resultFile.writeChars("\n");
                    line = "election leader success: once " + once + "times, twice " + twice + "times, " +
                            "three-times " + threeTimes + "times, more " + more + "times";
                    resultFile.writeChars(line);
                    resultFile.writeChars("\n");
                }
            }
        }
    }
}
