package utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.SaslOutputStream;
import utils.DoublyLinkedList.Node;

public class LRUCache<K,V> {

    private final Map<K,Pair<V,Node<K>>> mainMemory;
    private final DoublyLinkedList<K> lruQueue;
    private final int maxCapacity;
    private int size;
    private Pair<K,V> mostRecentlyUsed;

    public LRUCache(int capacity) {
        size = 0;
        maxCapacity = capacity;
        mainMemory = new HashMap<>(capacity+1,1.0f);
        lruQueue = new DoublyLinkedList<>();
    }

    public void put(K key, V value) {
        if(size == maxCapacity){
            var node = lruQueue.pop();
            mainMemory.remove(node.data);
            size--;
        }
        Node<K> node = lruQueue.add(key);
        size++;
        mainMemory.put(key,new Pair<>(value,node));
    }

    public V get(K key) {
        if(mostRecentlyUsed != null && mostRecentlyUsed.key.equals(key)){
            return mostRecentlyUsed.value;
        }
        Pair<V, Node<K>> entry;
        if((entry = mainMemory.get(key)) != null){
            Node<K> node = entry.value;
            lruQueue.remove(node);
            lruQueue.add(node);
            V toReturn = entry.key;
            mostRecentlyUsed = new Pair<>(key,toReturn);
            return toReturn;
        }
        return null;
    }

    public boolean contains(K key){
        return get(key) != null;
    }

    public static void main(String[] args){
        // Create a new LRUCache object with a capacity of 2
        LRUCache<Integer, String> cache = new LRUCache<>(2);

        // Add a large number of elements to the cache
        for (int i = 0; i < 10000; i++) {
            cache.put(i, "Value " + i);
        }

        // Try to retrieve an element that should be in the cache
        String value9999 = cache.get(9999);
        System.out.println("Value at key 9999: " + value9999);

        // Try to retrieve an element that should have been evicted from the cache
        String value0 = cache.get(0);
        System.out.println("Value at key 0: " + value0);
    }
}
