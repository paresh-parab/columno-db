package main.storage.page;

import main.catalog.Schema;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static main.common.Constants.COLUMN_SEP;
import static main.common.Constants.LINE_SEP;

public class TablePage extends Page
{
    private List<Tuple> data;
    private Schema schema;
    public Map<String, List<Value>> colData = new HashMap<>();
    public String tableName;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public TablePage(){
        resetMemory();
    }

    public void resetMemory(){
        data = new ArrayList<>();
    }

    public List<Tuple> getData(){
        return data;
    }

    public String toString() {
        StringBuilder res = new StringBuilder();

        res.append(super.toString());

        for(Tuple entry: data){
            res.append(entry.toString());
            res.append(LINE_SEP);
        }
        return res.substring(0, res.length()-LINE_SEP.length());
    }

    public void initializePageFromString(String input) {

        //assuming schema is already set
        super.initializePageFromString(input);

        String[] parts = input.split(LINE_SEP);

        resetMemory();

        List<TypeID> colTypes = schema.getColumnTypes();

        for(int i=5; i <parts.length; i++){
            String[] row = parts[i].split(COLUMN_SEP);
            List<Value> values = new ArrayList<>();
            for(int j=0; j< colTypes.size(); j++){
                switch(colTypes.get(j)){
                    case STRING_TYPE :
                        values.add(new Value(row[j]));
                        break;
                    case BOOLEAN_TYPE:
                        values.add(new Value(Boolean.valueOf(row[j])));
                        break;
                    case INTEGER_TYPE:
                        values.add(new Value(Integer.valueOf(row[j])));
                        break;
                }
            }
            data.add(new Tuple(values));

        }
    }
}
