package main.hash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ExtendibleHash<K, V> extends HashTable<K, V>
{
    private int globalDepth;
    int bucketSize, bucketNum;
    ArrayList<Bucket> buckets = new ArrayList<>();
    private final Lock mutex = new ReentrantLock(true);

    public class Bucket
    {
        public int localDepth;
        HashMap<K, V> kmap = new HashMap<>();
        private final Lock mutex = new ReentrantLock(true);

        public Bucket(int depth) { this.localDepth = depth; }
    }

    public ExtendibleHash() { new ExtendibleHash(64); }

    public ExtendibleHash(int size)
    {
        this.globalDepth = 0;
        this.bucketSize = size;
        this.bucketNum = 1;
        buckets.add(new Bucket(0));
    }

    public int HashKey(K key) { return (Math.abs(key.hashCode())) % bucketSize; }

    public int getGlobalDepth()
    {
        mutex.lock();
        int gdepth = globalDepth;
        mutex.unlock();
        return gdepth;
    }

    public int getLocalDepth(int bucketID)
    {
        if (buckets.contains(bucketID))
        {
            mutex.lock();

            if (buckets.get(bucketID).kmap.size() == 0) return -1;

            mutex.unlock();

            return buckets.get(bucketID).localDepth;
        }
        return -1;
    }

    public int getNumBuckets()
    {
        mutex.lock();
        int bNum = bucketNum;
        mutex.unlock();
        return bNum;
    }
    public boolean find(K key, V value)
    {
        int idx = getIdx(key);

        mutex.lock();

        if (buckets.get(idx).kmap.containsKey(key))
        {
            value = buckets.get(idx).kmap.get(key);

            return true;
        }
        mutex.unlock(); return false;
    }
    public void remove(K key)
    {
        int idx = getIdx(key);

        mutex.lock();

        Bucket current  = buckets.get(idx);

        if (!current.kmap.containsKey(key))

        current.kmap.remove(key);

        mutex.unlock();
    }

    public void insert(K key, V value)
    {
        int idx = getIdx(key);

        Bucket cur = buckets.get(idx);

        while (true)
        {
            mutex.lock();

            if (cur.kmap.containsKey(key) || cur.kmap.size() < bucketSize)
            {
                cur.kmap.put(key, value);
                break;
            }

            int mask = (1 << (cur.localDepth));
            cur.localDepth++;

            mutex.unlock();

            mutex.lock();
            if (cur.localDepth > globalDepth)
            {
                int length = buckets.size();

                for (int i = 0; i < length; i++)
                {
                    buckets.add(buckets.get(i));
                }
                globalDepth++;
            }
            bucketNum++;

            Bucket newBuc = new Bucket(cur.localDepth);

            Iterator<Map.Entry<K, V>> hmIterator = cur.kmap.entrySet().iterator();

            while (hmIterator.hasNext())
            {
                Map.Entry<K, V> mapElement = (Map.Entry) hmIterator.next();

                if ((HashKey(mapElement.getKey()) & mask) != 0)
                {
                    newBuc.kmap.put(mapElement.getKey(), mapElement.getValue());
                    cur.kmap.remove(mapElement);
                }
            }
            for (int i = 0; i < buckets.size(); i++)
            {
                if ((buckets.get(i) == cur && ((i & mask) != 0)))
                    buckets.set(i, newBuc);
            }
            idx = getIdx(key);
            cur = buckets.get(idx);
            mutex.unlock();
        }
    }

    public int getIdx(K key)
    {
        mutex.lock();
        int res = HashKey(key) & ((1 << globalDepth) - 1);
        mutex.unlock();
        return res;
    }
}
