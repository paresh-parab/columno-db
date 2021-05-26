package main.buffer;

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

    private int maxSize = 1000;
    private final Node<T> head = new Node<>();
    private final Node<T> tail = new Node<>();
    private final HashMap<T, Node<T>> map = new HashMap<>();
    private final Lock mutex = new ReentrantLock(true);

    public LRUReplacer()
    {
        head.next = tail;
        tail.prev = head;
    }

    public LRUReplacer(int maxSize)
    {
        this();
        this.maxSize = maxSize;
    }

    @Override
    public boolean pin(T frameID)
    {
        return erase(frameID);
    }

    @Override
    public void unpin(T frameID)
    {
        if(!map.containsKey(frameID)) insert(frameID);
    }

    @Override
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

    @Override
    public T victim(T value)
    {
        try
        {
            mutex.lock();

            Node<T> last = tail.prev;
            tail.prev = last.prev;
            last.prev.next = tail;

            T val = last.value;
            map.remove(last.value);

            mutex.unlock();
            return val;
        }
        catch (Exception e)
        {
            System.out.println("Exception: Error in getting the victim");
            return value;
        }
    }

    @Override
    public boolean erase(T value)
    {
        mutex.lock();

        if (map.containsKey(value))
        {
            Node<T> current = map.get(value);
            current.prev.next = current.next;
            current.next.prev = current.prev;
            map.remove(value);

            mutex.unlock();
            return true;
        }
        else
        {
            mutex.unlock();
            return false;
        }
    }

    @Override
    public int size()
    {
        mutex.lock();
        int size = map.size();
        mutex.unlock();
        return size;
    }

}
