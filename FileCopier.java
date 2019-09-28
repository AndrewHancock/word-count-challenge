import java.io.*;

public class FileCopier {

    private static String INPUT_FILENAME = "lorem.txt";

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        try (FileOutputStream output = new FileOutputStream("lorem_medium.txt")) {
            try (FileInputStream input = new FileInputStream("lorem.txt")) {
                byte[] buffer = new byte[(int)(new File(INPUT_FILENAME).length())];
                int bytesRead = 0;
                while (bytesRead != -1) {
                    bytesRead = input.read(buffer, bytesRead, buffer.length - bytesRead);
                }
                for (int i = 0; i < 100000; i++) {
                    output.write(buffer);
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.println((double) (end - start) / 1000 + " seconds.");

    }
}