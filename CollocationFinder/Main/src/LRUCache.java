import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LRUCache {

    private Map<String,String> mainMemory;
    private LinkedList<String> lruQueue;
    private int maxCapacity;
    private int size;

    public LRUCache(int capacity) {
        size = 0;
        maxCapacity = capacity;
        mainMemory = new HashMap<>(capacity+1,1.0f);
        lruQueue = new LinkedList<>();
    }

    public void put(String key, String value) {
        if(size == maxCapacity){
            String lruKey = lruQueue.remove(0);
            mainMemory.remove(lruKey);
            size--;
        }
        mainMemory.put(key,value);
        lruQueue.add(key);
        size++;
    }

}
