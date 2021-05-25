package main.execution.catalog;

import java.util.ArrayList;
import java.util.List;

public class Schema {
    List<Column> columns = new ArrayList<Column>();
    public Schema(List<Column> columns) {
        for(int i=0;i<columns.size();++i)
            this.columns.set(i,columns.get(i));
    }
}
