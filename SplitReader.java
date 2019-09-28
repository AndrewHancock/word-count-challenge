import java.io.*;
import java.nio.ByteBuffer;

class SplitReader implements AutoCloseable{
    private long startPos;
    private long endPos;

    private InputStream inputStream;
    private long totalBytesRead;
    private long totalBytesProcessed = 0;
    ByteBuffer byteBuffer;
    byte[] byteArray;
    public SplitReader(String fileName, long startPos, long endPos, int bufferSize) throws IOException {
        this.startPos = startPos;
        this.endPos = endPos;
        FileInputStream fileStream = new FileInputStream(new File(fileName));

        // Seek to the start of the split
        fileStream.skip(startPos);
        inputStream = new BufferedInputStream(fileStream);
        byteArray = new byte[bufferSize];
    }

    private ByteBuffer read() throws IOException {
        return read(0);
    }

    private ByteBuffer read(int startPos) throws IOException {
        int bytesRead = inputStream.read(byteArray, startPos, byteArray.length - startPos);
        totalBytesRead += bytesRead;
        limit = startPos + bytesRead;
        if(bytesRead == -1)
            return null;
        else
            return ByteBuffer.wrap(byteArray, 0, limit);
    }

    int position = -1;
    int limit = -1;
    /**
     *
     * @param whiteSpace Set to true if you want to consume whitespace, or false if you want to consume non-whitespace bytes
     * @return boolean which is true if EOF was reached, or false if EOF was not reached
     */
    private boolean eatBytes(boolean whiteSpace) throws IOException {
        boolean done = false;

        while(!done && !eof) {
            if(byteBuffer == null || position >= limit) {
                if(byteBuffer != null && tokenStart != -1) {
                    totalBytesProcessed -= limit - tokenStart;
                    byteBuffer.position(tokenStart);
                    byteBuffer.limit(limit);
                    byteBuffer.compact();
                    byteBuffer = read(byteBuffer.position());
                    if(byteBuffer != null) {
                        tokenStart = 0;
                        position = byteBuffer.position();
                        limit = byteBuffer.limit();
                    }

                }
                else {
                    byteBuffer = read();
                    if(byteBuffer != null) {
                        position = byteBuffer.position();
                        limit = byteBuffer.limit();
                    }
                }
                if(byteBuffer == null)
                    eof = true;
            }

            if (!eof) {

                int curByte = 0;
                do {
                    curByte = byteArray[position++];
                    totalBytesProcessed++;
                    boolean isWhitespace = (curByte == 10 || curByte == 13 || curByte == 32);
                    done = (isWhitespace && !whiteSpace) || (!isWhitespace && whiteSpace);
                }
                 while (!done && position < limit);

                if(done) {
                    position--;
                    totalBytesProcessed--;
                }
            }
        }
        return eof;
    }

    int tokenStart = -1;
    boolean consumedFirstToken = false;
    boolean eof = false;
    public ByteBuffer getToken() throws IOException {

        // We always read one token PAST EndPos.
        // This ensures the subsequent split can safely skip the first token.
        if(totalBytesProcessed + startPos > endPos ) {
            return null;
        }

        // If we are not the first split, and this is the first byte in the split, read and discard the first token.
        // This combined with the previous rule handles the case of tokens spanning splits.
        if(startPos != 0 && !consumedFirstToken) {
            consumedFirstToken = true;
            getToken();
        }

        tokenStart = -1;
        int tokenEnd = -1;

        // First, consume all preceding whitespace
        if (!eatBytes(true))  {
            tokenStart = position;
        }
        else {
            // No more tokens, EOF
            return null;
        }

        eof = eatBytes(false);
        tokenEnd = position;
        return ByteBuffer.wrap(byteArray, tokenStart, tokenEnd - tokenStart);
    }

    @Override
    public void close() throws Exception {
        if(inputStream != null) {
            inputStream.close();
        }
    }

    public long getStartPos() {
        return startPos;
    }

    public long getEndPos() {
        return endPos;
    }

}