package main.common;

import static main.common.Constants.COLUMN_SEP;

public class Pair<K, V>{

    private K key;
    private V value;


    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public Pair(Pair<K, V> p) {
        this(p.getKey(), p.getValue());
    }

    public Pair() {
        this(null, null);
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return key.toString() + COLUMN_SEP + value.toString();
    }

}