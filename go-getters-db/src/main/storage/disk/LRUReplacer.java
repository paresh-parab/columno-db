package main.storage.disk;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUReplacer<T> extends Replacer<T>
{
    static class Node<T>
    {
        private T value;

        public Node() {};
        public Node(T value) { this.value = value; }

        public Node<T> prev;
        public Node<T> next;
    }

    private Node<T> head;
    private Node<T> tail;
    private HashMap<T, Node<T>> map;
    private final Lock mutex = new ReentrantLock(true);

    public void insert(T value)
    {
        mutex.lock();

        Node<T> current;

        if (map.containsKey(value))
        {
            current = map.get(value);
            Node<T> prev = current.prev;
            Node<T> successor = current.next;
            prev.next = successor;
            successor.prev = prev;
        }
        else current = new Node(value);

        Node<T> first = head.next;

        current.next = first;
        first.prev = current;
        current.prev = head;
        head.next = current;

        map.put(value, current);

        mutex.unlock();
    }
    public void victim(T value)
    {
        mutex.lock();

        if (map.isEmpty()) return;

        Node<T> last = tail.prev;
        tail.prev = last.prev;
        last.prev.next = tail;
        map.remove(last.value);

        mutex.unlock();
    }

    public void erase(T value)
    {
        mutex.lock();
        if (map.containsKey(value))
        {
            Node<T> current = map.get(value);
            current.prev.next = current.next;
            current.next.prev = current.prev;
        }
        map.remove(value);

        mutex.unlock();
    }

    public int size()
    {
        mutex.lock();
        int size = map.size();
        mutex.unlock();
        return size;
    }

}
