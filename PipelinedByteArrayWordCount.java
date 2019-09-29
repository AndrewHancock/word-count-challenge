import producerconsumer.ByteArrayProducer;
import producerconsumer.ByteArrayTokenizer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PipelinedByteArrayWordCount {
    public static void main(String[] args) throws IOException {
        try(FileInputStream input = new FileInputStream("lorem.txt")) {
            ByteArrayProducer reader = new ByteArrayProducer(input, 16);
            ByteArrayTokenizer tokenizer = new ByteArrayTokenizer();

            byte[] array = null;
            while((array = reader.read()) != null) {
                tokenizer.setArray(array);
                System.out.printf("File pos: %d Array Length: %d String: %s\nTokens: ", reader.getBytesRead(), array.length, new String(array, "UTF-8"));
                byte[] token;
                while((token = tokenizer.nextToken()) != null) {
                    System.out.print(new String(token, "UTF-8") + " ");
                }
                System.out.printf("\n\n");
            }
        }
    }
}
