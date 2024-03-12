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
        LRUCache<String, Integer> cache = new LRUCache<>(2);

        // Add some key-value pairs to the cache
        cache.put("one", 1);
        cache.put("two", 2);

        // Retrieve the values for the keys added and print them to the console
        System.out.println("Value for 'one': " + cache.get("one"));
        System.out.println("Value for 'two': " + cache.get("two"));

        // Check if a certain key exists in the cache and print the result to the console
        System.out.println("Cache contains 'one': " + cache.contains("one"));
        System.out.println("Cache contains 'two': " + cache.contains("two"));

        // Add another key-value pair to the cache
        cache.put("three", 3);

        System.out.println("Cache contains 'one': " + cache.contains("one"));
        System.out.println("Cache contains 'two': " + cache.contains("two"));
        System.out.println("Cache contains 'three': " + cache.contains("three"));

        // Try to retrieve the value for the key 'one' and print it to the console
        System.out.println("Value for 'one' after adding 'three': " + cache.get("one"));
        System.out.println("Value for 'two' after adding 'three': " + cache.get("two"));
        System.out.println("Value for 'three' after adding 'three': " + cache.get("three"));
        cache.get("three");
    }
}
