package producerconsumer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteArrayProducer {
    private InputStream inputStream;
    private long bytesRead;

    byte[] array;

    public ByteArrayProducer(InputStream inputStream, int bufferSize) throws FileNotFoundException {
        this.inputStream = inputStream;
        array = new byte[bufferSize];
    }

    int remainder;
    public byte[] read() throws IOException {
        int bytesRead = inputStream.read(array, remainder, array.length - remainder);
        this.bytesRead += bytesRead;
        if(bytesRead == -1)
            return null;
        int limit = bytesRead + remainder;

        int i = limit -1;
        for(;i > 0 && array[i] != 32 && array[i] != 10 && array[i] !=13; i--);

        byte[] copy = new byte[i];
        System.arraycopy(array, 0, copy, 0,i );

        if(i != limit - 1) {
            remainder = limit - i - 1 ;
            System.arraycopy(array, i + 1, array, 0, remainder);
        }
        else {
            remainder = 0;
        }
        return copy;
    }

    public long getBytesRead() {
        return bytesRead;
    }
}
