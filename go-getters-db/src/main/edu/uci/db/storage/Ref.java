package main.edu.uci.db.storage;

public class Ref<T>
{
    private T value;

    public Ref(T value)
    {
        this.value = value;
    }
    public T getValue(){
        return value;
    }

    public void setValue(T value){
        this.value = value;
    }
}