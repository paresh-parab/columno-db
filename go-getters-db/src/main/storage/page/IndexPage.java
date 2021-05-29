package main.storage.page;

import main.common.Pair;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.COLUMN_SEP;
import static main.common.Constants.LINE_SEP;

public class IndexPage<KeyType, ValueType> extends Page{
    private List<Pair<KeyType, ValueType>> data;

    public IndexPage(){
        resetMemory();
    }

    public void resetMemory(){
        data = new ArrayList<>();
    }

    public List<Pair<KeyType, ValueType>> getData(){
        return data;
    }


    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(super.toString());
        for(Pair<KeyType, ValueType> entry: data){
            res.append(entry.toString());
            res.append(LINE_SEP);
        }
        return res.substring(0, res.length()-LINE_SEP.length());
    }

    public void initializePageFromString(String input) {

        super.initializePageFromString(input);

        String[] parts = input.split(LINE_SEP);

        resetMemory();

        for(int i=5; i <parts.length; i++){
            String[] row = parts[i].split(COLUMN_SEP);
            String[] pairParts = parts[i].split(COLUMN_SEP);
            data.add(new Pair((KeyType)pairParts[0], (ValueType) pairParts[1]));
        }

        return;
    }
}
