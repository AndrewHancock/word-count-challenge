import producerconsumer.ByteArrayProducer;
import producerconsumer.ByteArrayTokenizer;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class PipelinedByteArrayWordCount {
    public static Map<String, Integer> run(String fileName, int threadCount, int bufferSize) {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            BlockingQueue<byte[]> byteArrayQueue = new ArrayBlockingQueue<>(threadCount);

            Collection<Future<Map<String, Integer>>> countFutures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                countFutures.add(executorService.submit(() -> {
                    ByteArrayTokenizer tokenizer = new ByteArrayTokenizer();
                    Map<ByteBuffer, Integer> counts = new HashMap<>(1024);
                    byte[] array;
                    while ((array = byteArrayQueue.take()).length != 0) {
                        tokenizer.setArray(array);
                        byte[] token;
                        while ((token = tokenizer.nextToken()) != null) {
                            ByteBuffer buffer = ByteBuffer.wrap(token);
                            counts.merge(buffer, 1, Integer::sum);
                        }
                    }

                    return counts.entrySet().stream()
                            .collect(Collectors.toMap(e -> {
                                        try {
                                            return new String(e.getKey().array(), "UTF-8");
                                        } catch (Exception ex) {
                                            System.out.print(e);
                                            System.exit(1);
                                        }
                                        return null;
                                    },
                                    e -> e.getValue()));
                }));

            }

            try (FileInputStream input = new FileInputStream(fileName)) {
                ByteArrayProducer reader = new ByteArrayProducer(input, bufferSize);
                ByteArrayTokenizer tokenizer = new ByteArrayTokenizer();

                byte[] array = null;
                while ((array = reader.read()) != null) {
                    byteArrayQueue.put(array);
                }

                // Send shutdown signal to each thread
                for (int i = 0; i < threadCount; i++) {
                    byteArrayQueue.put(new byte[0]);
                }
            }

            // Block on task completion and accumulate results
            Map<String, Integer> totalCounts = new HashMap<>();
            for (Future<Map<String, Integer>> subCounts : countFutures) {
                for (Map.Entry<String, Integer> entry : subCounts.get().entrySet()) {
                    int count = totalCounts.getOrDefault(entry.getKey(), 0);
                    totalCounts.put(entry.getKey(), (count + entry.getValue()));
                }
            }
            executorService.shutdown();
            return totalCounts;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
