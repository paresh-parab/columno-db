package main.storage.table;

import main.catalog.Schema;
import main.common.StringInitializable;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.COLUMN_SEP;

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

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        for(Value v: content){
            res.append(v.toString());
            res.append(COLUMN_SEP);
        }
        res.deleteCharAt(res.length()-1);
        return res.toString();
    }

}