package main.execution.queryExecutor;

import main.catalog.Schema;
import main.storage.page.Page;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.COLUMN_SEP;
import static main.common.Constants.LINE_SEP;

public class queryTablePage extends Page {

    private List<Tuple> data;
    private Schema schema;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public queryTablePage() {
        resetMemory();
    }

    public void resetMemory() {
        data = new ArrayList<>();
    }

    public List<Tuple> getData() {
        return data;
    }


    public String toString() {
        StringBuilder res = new StringBuilder();

        res.append(super.toString());

        res.append(schema.getColumnTypes());
        res.append(LINE_SEP);

        for (Tuple entry : data) {
            res.append(entry.toString());
            res.append(LINE_SEP);
        }
        res.deleteCharAt(res.length() - 1);
        return res.toString();
    }

    public void initializePageFromString(String input) {

        super.initializePageFromString(input);

        String[] parts = input.split(String.valueOf(LINE_SEP));

        List<TypeID> typeList = new ArrayList<>();

        String[] types = parts[5].split(String.valueOf(COLUMN_SEP));

        for (String t : types) {
            typeList.add(TypeID.valueOf(t));
        }

        resetMemory();

        for (int i = 6; i < parts.length; i++) {
            String[] row = parts[i].split(String.valueOf(COLUMN_SEP));
            List<Value> values = new ArrayList<>();
            for (int j = 0; j < typeList.size(); j++) {
                switch (typeList.get(j)) {
                    case STRING_TYPE:
                        values.add(new Value(row[j]));
                        break;
                    case BOOLEAN_TYPE:
                        values.add(new Value(Integer.valueOf(row[j])));
                        break;
                    case INTEGER_TYPE:
                        values.add(new Value(Boolean.valueOf(row[j])));
                        break;
                }
            }
            data.add(new Tuple(values));

        }

        return;
    }
}
