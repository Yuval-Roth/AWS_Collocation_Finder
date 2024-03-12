package utils;

import java.util.HashMap;
import java.util.Map;
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
        mainMemory.put(key,new Pair<>(value,node));
        size++;
    }

    public V get(K key) {
        if(mostRecentlyUsed != null && mostRecentlyUsed.key.equals(key)){
            return mostRecentlyUsed.value;
        }
        Pair<V, Node<K>> entry;
        if((entry = mainMemory.get(key)) != null){
            Node<K> node = entry.value;
            lruQueue.remove(node);
            lruQueue.add(key);
            V toReturn = entry.key;
            mostRecentlyUsed = new Pair<>(key,toReturn);
            return toReturn;
        }
        return null;
    }

    public boolean contains(K key){
        return get(key) != null;
    }
}
