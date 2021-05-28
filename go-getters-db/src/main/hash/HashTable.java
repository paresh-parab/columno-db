package main.hash;

public abstract class HashTable<K, V>
{
    public abstract void insert(K key, V value);

    public abstract V find(K pageID, V tar);

    public abstract boolean remove(K pageID);
}
