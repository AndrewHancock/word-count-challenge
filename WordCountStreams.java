import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class WordCountStreams {
    public static Map<String, Integer> run(String fileName) {
        try {
            return Files.lines(Paths.get(fileName)).parallel()
                    .flatMap(x -> Arrays.stream(x.split(" ")))
                    .filter(w -> !w.isEmpty())
                    .collect(Collectors.toMap(word -> word, word -> 1, Integer::sum));
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
