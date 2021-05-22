package main.storage.disk;

public abstract class Replacer<T>
{
    public Replacer(){};

    public abstract void victim(T frameID);

    public void pin(T frameID){};

    public void unpin(T frameID){};

    public abstract int size();

    public abstract void erase(T tar);

    public abstract void insert(T tar);
}
