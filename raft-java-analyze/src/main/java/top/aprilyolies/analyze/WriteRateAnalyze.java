package top.aprilyolies.analyze;


import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.googlecode.protobuf.format.JsonFormat;
import lombok.SneakyThrows;
import top.aprilyolies.raft.example.server.service.ExampleProto;
import top.aprilyolies.raft.example.server.service.ExampleService;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class WriteRateAnalyze {
    private static JsonFormat jsonFormat = new JsonFormat();
    private static CountDownLatch latch;

    @SneakyThrows
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.printf("Usage: ./run_concurrent_client.sh THREAD_NUM\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);

        int node_num = 3;
        int times = 50;    // 统计次数
        int clientNums = 1; // 客户端数量
        int optNum = 1000;  // 写入记录数

        ExecutorService readThreadPool = Executors.newCachedThreadPool();
        ExecutorService writeThreadPool = Executors.newCachedThreadPool();

        File file = new File("raft-java-analyze/write_rate/node_num_" + node_num);
        if (file.exists()) file.delete();
        if (!file.createNewFile()) {
            System.out.println("create new file failed");
            return;
        }

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        String line = "node_num: 3, records_num: 1000\n";
        raf.writeChars(line);

        for (int j = 0; j < 8; j++) {
            int totalTime = 0;  // 统计总耗时
            for (int i = 0; i < times; i++) {
                latch = new CountDownLatch(clientNums);
                Future<?>[] future = new Future[clientNums];
                long start = System.currentTimeMillis();
                for (int k = 0; k < clientNums; k++) {
                    future[k] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool, optNum / clientNums));
                }
                latch.await();
                long cost = System.currentTimeMillis() - start;
                totalTime += cost;
                System.out.println("Write " + optNum + " records with " + clientNums + " clients cost " + cost + "ms");
            }
            line = "Write " + optNum + " records with " + clientNums + " clients average cost " + totalTime / times + "ms\n";
            raf.writeChars(line);
            clientNums += 2;
        }

        readThreadPool.shutdown();
        writeThreadPool.shutdown();
    }

    public static class SetTask implements Runnable {
        private final int optNum;
        private ExampleService exampleService;
        ExecutorService readThreadPool;

        public SetTask(ExampleService exampleService, ExecutorService readThreadPool, int optNum) {
            this.exampleService = exampleService;
            this.readThreadPool = readThreadPool;
            this.optNum = optNum;
        }

        @Override
        public void run() {
            for (int i = 0; i < optNum; i++) {
                String key = UUID.randomUUID().toString();
                String value = UUID.randomUUID().toString();
                ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
                        .setKey(key).setValue(value).build();

                long startTime = System.currentTimeMillis();
                ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
//                try {
//                    if (setResponse != null) {
//                        System.out.printf("set request, key=%s, value=%s, response=%s, elapseMS=%d\n",
//                                key, value, jsonFormat.printToString(setResponse), System.currentTimeMillis() - startTime);
//                        readThreadPool.submit(new GetTask(exampleService, key));
//                    } else {
//                        System.out.printf("set request failed, key=%s value=%s\n", key, value);
//                    }
//                } catch (Exception ex) {
//                    ex.printStackTrace();
//                }
            }
            latch.countDown();
        }
    }

    public static class GetTask implements Runnable {
        private ExampleService exampleService;
        private String key;

        public GetTask(ExampleService exampleService, String key) {
            this.exampleService = exampleService;
            this.key = key;
        }

        @Override
        public void run() {
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
            long startTime = System.currentTimeMillis();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            try {
                if (getResponse != null) {
                    System.out.printf("get request, key=%s, response=%s, elapseMS=%d\n",
                            key, jsonFormat.printToString(getResponse), System.currentTimeMillis() - startTime);
                } else {
                    System.out.printf("get request failed, key=%s\n", key);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
