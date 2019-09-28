import java.io.*;
import java.nio.ByteBuffer;

class SplitReader implements AutoCloseable{
    private long startPos;
    private long endPos;
    private long bytesProcessed;
    private InputStream inputStream;

    byte[] buffer;
    public SplitReader(String fileName, long startPos, long endPos, int bufferSize) throws IOException {
        this.startPos = startPos;
        this.endPos = endPos;
        FileInputStream fileStream = new FileInputStream(new File(fileName));

        // Seek to the start of the split
        fileStream.skip(startPos);
        inputStream = fileStream;
        buffer = new byte[bufferSize];
    }

    int position = -1;
    int limit = -1;
    int tokenStart = -1;
    int tokenEnd = -1;

    /**
     * We are in the middle of a token.
     * Copy the current portion of the token to the beginning of the buffer and reset the tokenStart and position.
     */
    private void compactPartialToken() {
        int partialTokenLength = position - tokenStart;
        System.arraycopy(buffer, tokenStart, buffer, 0, partialTokenLength);
        tokenStart = 0;
        position = partialTokenLength;
    }

    private void fillBuffer() throws IOException {
        if(tokenStart != -1) {
            compactPartialToken();
        }
        else {
            position = 0;
        }
        int bytesRead = inputStream.read(buffer, position, buffer.length - position);
        if(bytesRead == -1) {
            limit = -1;
        }
        else {
            limit = position + bytesRead;
        }
    }

    boolean consumedFirstToken = false;
    boolean consumedExtraToken = false;

    public byte[] getToken() throws IOException {

        if(consumedExtraToken) {
            return null;
        }

        if(startPos != 0 && !consumedFirstToken) {
            consumedFirstToken = true;
            getToken();
        }

        while(tokenStart == -1 || tokenEnd == -1) {
            if(position == limit) {
                fillBuffer();
            }

            if(limit != -1) {
                byte currByte = buffer[position];
                if (tokenStart == -1) {
                    if (currByte != 32 && currByte != 10 && currByte != 13) {
                        tokenStart = position;
                    }
                    position++;
                    bytesProcessed++;
                }
                else {
                    if (currByte == 32 || currByte == 10 | currByte == 13) {
                        tokenEnd = position - 1;
                    }
                    position++;
                    bytesProcessed++;
                }
            }
            else {
                if(tokenStart != -1) {
                    tokenEnd = position;
                }
                else {
                    return null; // EOF, no token.
                }
            }
        }

        int tokenLength = tokenEnd - tokenStart + 1;
        byte[] token = new byte[tokenLength];
        System.arraycopy(buffer, tokenStart, token, 0, tokenLength);

        if(startPos + bytesProcessed -2 > endPos) {
            consumedExtraToken = true;
        }

        tokenStart = -1;
        tokenEnd = -1;
        return token;
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