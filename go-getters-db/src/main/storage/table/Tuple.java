package main.storage.table;

import main.type.Value;

import java.util.List;

public class Tuple {

    private List<Value> content;

    public Tuple(List<Value> content) {
        this.content = content;
    }

    public List<Value> getContent() {
        return content;
    }

    public void setContent(List<Value> content) {
        this.content = content;
    }

    public Value getValue(int colIdx){
        return content.get(colIdx);
    }

}