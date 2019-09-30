import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public class Bench {
    private static class BenchmarkRun {
        String name;
        long start;
        long end;

        // The Benchmark Function takes a filename as a string, and returns a Map of word frequencies
        Function<String, Map<String, Integer>> benchFunc;

        public BenchmarkRun(String name, Function<String, Map<String, Integer>> benchFunc) {
            this.name = name;
            this.benchFunc = benchFunc;
        }
    }

    public static void main(String[] args) {

        Collection<BenchmarkRun> benchmarks = new ArrayList<>(Arrays.asList (
                new BenchmarkRun("Producer-Consumer pipeline using byte arrays and byte buffers",
                        (fileName) -> PipelinedByteArrayWordCount.run(fileName, 15, 16 * 1024 * 1024)),
                new BenchmarkRun("Parallel Java Streams", (fileName) -> WordCountStreams.run(fileName)),
                new BenchmarkRun("Parallel reader threads using byte arrays and byte buffers",
                        (fileName) -> WordCount.run(fileName))
                )

        );

        for(BenchmarkRun run : benchmarks) {
            System.out.println(run.name + ":");
            for(int i = 0; i < 10; i++) {
                long start = System.currentTimeMillis();
                run.benchFunc.apply("lorem_medium.txt");
                long end = System.currentTimeMillis();
                System.out.println("Run #" + (i + 1) + ":" + (double)(end-start)/1000 );
            }
            System.out.println();
        }
    }
}
