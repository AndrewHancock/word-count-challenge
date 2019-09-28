import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class WordCountStreams {
    public static void main(String[] args) throws IOException {
        System.out.println("Press any key to continue...");
        System.in.read();
        long start = System.currentTimeMillis();
        Map<String, Long> counts = Files.lines( Paths.get("lorem_medium.txt")).parallel()
                .flatMap(x -> Arrays.stream(x.split(" ")))
                .filter( w -> !w.isEmpty())
            .collect(Collectors.toMap(word -> word, word -> 1L, Long::sum));

        long end = System.currentTimeMillis();

        counts.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map( x-> {
                    System.out.printf("%s ==> %d\n", x.getKey(), x.getValue());
                    return x;
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        System.out.println("Seconds: " + (double)(end - start) /1000);
    }
}
