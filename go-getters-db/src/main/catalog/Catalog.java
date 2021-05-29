package main.catalog;

import main.buffer.BufferPoolManager;
import main.storage.index.BPlusTree;
import main.storage.table.TableHeap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Catalog {

    public class TableMetadata {
        private Schema schema;
        private String name;
        private TableHeap table;
        private int oid;

        TableMetadata(Schema schema, String name, TableHeap table, int oid) {
            this.schema = schema;
            this.name = name;
            this.table = table;
            this.oid = oid;
        }


        public Schema getSchema() {
            return schema;
        }

        public void setSchema(Schema schema) {
            this.schema = schema;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public TableHeap getTable() {
            return table;
        }

        public void setTable(TableHeap table) {
            this.table = table;
        }

        public int getOid() {
            return oid;
        }

        public void setOid(int oid) {
            this.oid = oid;
        }
    }

    private BufferPoolManager bpm;

    /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
    private Map<Integer, TableMetadata> tables = new HashMap<>();
    /** names_ : table names -> table identifiers */
    private Map<String, Integer> names = new HashMap<>();
    /** The next table identifier to be used. */
    private int nextTableOID = 0;


    /** indices: storing table id against column and index root page ID map entries */
    private Map<Integer, Map<Column, BPlusTree>> indices = new HashMap<>();

    public Catalog(BufferPoolManager bpm) {
        this.bpm = bpm;
    }

    /**
     * Create a new table and return its metadata.
     * @param txn the transaction in which the table is being created
     * @param table_name the name of the new table
     * @param schema the schema of the new table
     * @return a pointer to the metadata of the new table
     */
    public TableMetadata createTable(String tableName, Schema schema) {
        try{
            if(names.get(tableName) != null)
                throw new Exception("Table names should be unique! ");
            int tableOID = nextTableOID;
            nextTableOID++;
            names.put(tableName, tableOID);
            tables.put( tableOID, new TableMetadata( schema, tableName, new TableHeap(bpm, schema), tableOID));
            return tables.get(tableOID);

        }catch( Exception e){
            System.out.println("Program terminated due to exception: " + e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }

    /** @return table metadata by name */
    public TableMetadata getTable(String tableName) {
        try{
            if (names.get(tableName) == null) {
                throw new Exception("Can't find table: " + tableName);
            }
            return tables.get(names.get(tableName));
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }

    /** @return table metadata by oid */
    public TableMetadata getTable(int tableOID) {
        try{
            if (tables.get(tableOID) == null ) {
                throw new Exception("Can't find table oid: " + tableOID);
            }
            return tables.get(tableOID);
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }

    public boolean createIndex(int tableOID, String column){

        if( !tables.containsKey(tableOID) )
            //table OID is invalid
            return false;
        List<Column> columnList = tables.get(tableOID).schema.getColumns();
        Column targetColumn = null;
        for(Column c: columnList){
            if(c.getName().equals(column)){
                targetColumn = c;
            }
        }

        if(targetColumn == null)
            return false;
        BPlusTree tree = getBPlusTree(targetColumn);
        Map<Column, BPlusTree> indexMap;

        if(indices.containsKey(tableOID)){
            indexMap = indices.get(tableOID);
        }else{
            indexMap = new HashMap<>();
        }

        indexMap.put(targetColumn, tree);
        return true;
    }

    private BPlusTree getBPlusTree(Column c){
        switch (c.getType()){
            case STRING_TYPE :
                return new BPlusTree<String, Integer, Comparator<String>>(c.getName(), bpm, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });
            case INTEGER_TYPE:
                return new BPlusTree<Integer, Integer, Comparator<Integer>>(c.getName(), bpm, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o1.compareTo(o2);
                    }
                });
        }
        return null;
    }

    public Map<Integer, TableMetadata> getTables() {
        return tables;
    }

    public Map<String, Integer> getNames() {
        return names;
    }

    public int getNextTableOID() {
        return nextTableOID;
    }
}