import java.io.*;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class WordCount{


    private static Collection<SplitReader> generateSplitReaders(String fileName, int numberOfSplits, int bufferSize) throws IOException {
        Collection<SplitReader> result = new ArrayList<>();
        File file = new File(fileName);
        long splitSize = file.length() / numberOfSplits;

        long lastPos = 0;
        for(int i = 0; i < numberOfSplits; i++) {
            long startPos = 0;
            if(lastPos != 0) {
                startPos = lastPos + 1;
            }
            long endPos = startPos + splitSize;

            // To handle any remainder bytes, the last split is set to the end of the file
            if(i == numberOfSplits - 1)
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

    private static Map<String, Integer> countWords(SplitReader tokenReader) throws IOException {
        Map<String, Integer> counts = new HashMap<>(1024);
        byte[] token = null;
        while ((token = tokenReader.getToken()) != null) {
            String tokenStr = new String(token, "UTF-8");
            int value = counts.getOrDefault(tokenStr, 0);
            counts.put(tokenStr, value + 1);
            //counts.merge(new String(token, "UTF-8"), 1, Integer::sum);

        }

        return counts;
        /*return counts.entrySet().stream()
                .collect(Collectors.toMap(e -> {
                            try {
                                return byteBufferToString(e.getKey());
                            } catch (Exception ex) {
                                System.out.print(e);
                                System.exit(1);
                            }
                            return null;
                        },
                        e -> e.getValue()));*/
    }


    private static int THREAD_COUNT =16;
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        System.out.println("Press any key...");
        System.in.read();
        long start = System.currentTimeMillis();
        Collection<SplitReader> readers = generateSplitReaders("lorem_large.txt", THREAD_COUNT, 16 * 1024 * 1024);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Submit splits threads for execution
        Collection<Future<Map<String, Integer>>> subCountFutures = new ArrayList<>();
        for(SplitReader reader : readers) {
            subCountFutures.add(executor.submit(() ->  {
                Map<String, Integer> totals = countWords(reader);
                reader.close();
                return totals;
            }));
        }

        // Block on task completion and accumulate results
        Map<String, Integer> totalCounts = new HashMap<>();
        for(Future<Map<String, Integer>> subCounts : subCountFutures) {
            for (Map.Entry<String, Integer> entry : subCounts.get().entrySet()) {
                int count = totalCounts.getOrDefault(entry.getKey(), 0);
                totalCounts.put(entry.getKey(), (count + (Integer)entry.getValue()));
            }
        }
        executor.shutdown();
        long end = System.currentTimeMillis();

        totalCounts.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map( x-> {
                    System.out.printf("%s ==> %d\n", x.getKey(), x.getValue());
                    return x;
                })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        System.out.println("Seconds: " + ((double)(end - start) / 1000));


        for(Map.Entry<String, Integer> entry : totalCounts.entrySet()) {

        }


    }
}