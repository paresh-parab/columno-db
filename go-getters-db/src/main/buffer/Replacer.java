package main.buffer;

public abstract class Replacer<T>
{
    public Replacer(){};

    public abstract T victim(T frameID);

    public abstract boolean pin(T frameID);

    public abstract void unpin(T frameID);

    public abstract int size();

    public abstract boolean erase(T tar);

    public abstract void insert(T tar);
}
