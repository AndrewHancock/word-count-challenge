package producerconsumer;

public class ByteArrayTokenizer {
    private byte[] array;
    int position;
    int length;
    public void setArray(byte[] array) {
        this.array = array;
        length =array.length;
        position = 0;
        endOfTokens = false;
    }

    boolean endOfTokens = false;
    public byte[] nextToken() {
        int tokenStart = -1;
        int tokenEnd = -1;

        if(endOfTokens) {
            return null;
        }


        while( position < length && (tokenEnd == -1 || tokenStart == -1 )) {
            byte ch = array[position];
            if(tokenStart == -1 && ch != 32 && ch != 10 && ch !=13) {
                tokenStart = position;
            }
            else if(tokenStart != -1 && (ch == 32 || ch == 10 || ch == 13)) {
                tokenEnd = position - 1;
            }
            position ++;
        }
        int tokenLength = 0;
        if(position == length) {
            tokenLength = position - tokenStart;
            endOfTokens = true;
            if(tokenStart == -1) {
                return null;
            }
            if(tokenEnd == -1) {
                tokenEnd = position - 1;
            }
        }
        else {
            tokenLength = tokenEnd - tokenStart;
        }

        byte[] tokenArray = new byte[tokenLength];
        System.arraycopy(array, tokenStart, tokenArray, 0, tokenLength);
        return tokenArray;
    }
}
