import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

public class Accumulator<T extends Comparable> {
    private int bufferSize;
    private ArrayList<AbstractMap.SimpleEntry<T, Integer>> swapBuffer;
    private ArrayList<AbstractMap.SimpleEntry<T, Integer>> buffer;

    public Accumulator(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = new ArrayList<>(bufferSize);
        swapBuffer = new ArrayList<>(bufferSize);
    }

    public void addCount(T key, int value) {
        if(buffer.size() == bufferSize) {
            mergeCounts();
        }
        buffer.add(new AbstractMap.SimpleEntry<T, Integer>(key, value));
        merged = false;
    }

    public Collection<AbstractMap.SimpleEntry<T, Integer>> getCounts() {
        if(!merged) {
            mergeCounts();
        }
        return buffer;
    }

    private boolean merged;
    private void mergeCounts() {
        buffer.sort(Comparator.comparing(AbstractMap.SimpleEntry::getKey));

        swapBuffer.clear();
        AbstractMap.SimpleEntry<T, Integer> prev = null;
        for(AbstractMap.SimpleEntry<T, Integer> entry : buffer) {
            if(prev != null) {
                if(prev.getKey().equals(entry.getKey())) {
                    int value = prev.getValue();
                    entry.setValue(value + entry.getValue());
                }
                else {
                    swapBuffer.add(prev);
                }
            }
            prev = entry;
        }
        // Due to loop termination condition, we must add the last element after the loop.
        swapBuffer.add(prev);

        // Swap the buffers
        ArrayList<AbstractMap.SimpleEntry<T, Integer>> temp = buffer;
        buffer = swapBuffer;
        swapBuffer = temp;

        merged = true;
    }
}
