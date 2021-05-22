package main.hash;

public abstract class HashTable<KeyType, ValueType>
{
    public abstract void insert(KeyType key, ValueType value);
    public abstract boolean find(KeyType pageID, ValueType tar);

    public abstract void remove(KeyType pageID);
}
