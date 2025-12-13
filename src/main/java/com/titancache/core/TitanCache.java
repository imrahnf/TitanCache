package com.titancache.core;
import com.titancache.api.CacheMetrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.Map;

public class TitanCache<K, V> {
    // Thread safe counters
    private final AtomicInteger hits = new AtomicInteger(0);
    private final AtomicInteger misses = new AtomicInteger(0);
    private final AtomicInteger evictions = new AtomicInteger(0);

    private int capacity;
    private Map<K, CacheNode<K, V>> map;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Sentinel nodes
    CacheNode<K, V> head;
    CacheNode<K, V> tail;

    // Ask for stats
    public CacheMetrics getMetrics() {
        int totalHits = hits.get();
        int totalMisses = misses.get();
        int totalEvictions = evictions.get();

        double ratio = (totalHits + totalMisses) == 0 ? 0.0 : (double) totalHits / (totalHits + totalMisses);

        return new CacheMetrics(totalHits, totalMisses, totalEvictions, ratio);
    }

    // Search for the key. If it exists, place it at the front of the recently used list.
    public V get(K key) {
        lock.writeLock().lock(); // writeLock as we still add/remove nodes due to the nature of LRU
        try {
            if (map.containsKey(key)) {
                // Remove the node and place at front
                CacheNode<K, V> temp = removeNode(map.get(key));
                addNode(temp);

                hits.incrementAndGet();
                return temp.value;
            } else {
                misses.incrementAndGet();
                return null;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            // If it exists, update
            if (map.containsKey(key)) {
                CacheNode<K, V> node = map.get(key);
                node.value = value;
                removeNode(node);
                addNode(node);
                return;
            }

            // New Key. Check if cache full
            if (map.size() == capacity) {
                // Get LRU
                CacheNode<K, V> lru = tail.prev;

                removeNode(lru);
                evictions.incrementAndGet();
                map.remove(lru.key);
            }

            // New key & create
            CacheNode<K, V> node = new CacheNode<>(key, value);
            addNode(node);
            map.put(key, node);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Constructor
    public TitanCache(int capacity) {
        this.capacity = capacity;
        this.map = new HashMap<>();

        // Create dummies
        this.head = new CacheNode<>(null, null);
        this.tail = new CacheNode<>(null, null);

        // Setup sentinel nodes
        head.next = tail;
        tail.prev = head;
    }

    private void addNode(CacheNode<K, V> node) {
        // Get the first node
        CacheNode<K, V> oldNext = head.next;

        // Link nodes
        node.prev = head;
        node.next = oldNext;
        head.next = node;
        oldNext.prev = node;
    }

    private CacheNode<K, V> removeNode(CacheNode<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        return node;
    }
}
