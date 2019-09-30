import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MultipleReaderCounterWordCount {


    private static Collection<SplitReader> generateSplitReaders(String fileName, int numberOfSplits, int bufferSize) throws IOException {
        Collection<SplitReader> result = new ArrayList<>();
        File file = new File(fileName);
        long splitSize = file.length() / numberOfSplits;

        long lastPos = 0;
        for (int i = 0; i < numberOfSplits; i++) {
            long startPos = 0;
            if (lastPos != 0) {
                startPos = lastPos + 1;
            }
            long endPos = startPos + splitSize;

            // To handle any remainder bytes, the last split is set to the end of the file
            if (i == numberOfSplits - 1)
                endPos = file.length();

            result.add(new SplitReader(fileName, startPos, endPos, bufferSize));
            lastPos = endPos;
        }
        return result;
    }

    private static String byteBufferToString(ByteBuffer buffer) throws UnsupportedEncodingException {
        byte[] bytes = new byte[buffer.limit() - buffer.position()];
        buffer.mark();
        buffer.get(bytes);
        buffer.reset();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Map<String, Integer> countWords(SplitReader reader) throws IOException, InterruptedException {
        Map<ByteBuffer, Integer> counts = new HashMap<>(1024);
        byte[] token;
        while ((token = reader.getToken()) != null) {
            ByteBuffer tokenBuf = ByteBuffer.wrap(token);
            counts.merge(tokenBuf, 1, Integer::sum);
        }

        return counts.entrySet().stream()
                .collect(Collectors.toMap(e -> {
                            try {
                                return byteBufferToString(e.getKey());
                            } catch (Exception ex) {
                                System.out.print(e);
                                System.exit(1);
                            }
                            return null;
                        },
                        e -> e.getValue()));
    }

    public static Map<String, Integer> run(String fileName, int threadCount, int buffer) {
        try {
            Collection<SplitReader> readers = generateSplitReaders(fileName, threadCount, buffer);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            Collection<Future<Map<String, Integer>>> subCountFutures = new ArrayList<>();

            for (SplitReader reader : readers) {
                subCountFutures.add(executor.submit(() -> countWords(reader)));
            }

            // Block on task completion and accumulate results
            Map<String, Integer> totalCounts = new HashMap<>();
            for (Future<Map<String, Integer>> subCounts : subCountFutures) {
                for (Map.Entry<String, Integer> entry : subCounts.get().entrySet()) {
                    int count = totalCounts.getOrDefault(entry.getKey(), 0);
                    totalCounts.put(entry.getKey(), (count + (Integer) entry.getValue()));
                }
            }
            executor.shutdown();
            return totalCounts;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}