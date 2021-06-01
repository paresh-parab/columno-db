package test.storage.catalog;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Column;
import main.catalog.Schema;
import main.storage.disk.DiskManager;
import main.type.TypeID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CatalogTest {

    @org.junit.jupiter.api.Test
    public void testTableCreation(){

        List<Column> cols = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("IsAdult", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s = new Schema(cols);
        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");

        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager);

        Catalog catalog = new Catalog(bufferPoolManager);

        Catalog.TableMetadata tableMetadata = catalog.createTable("FirstTable", s);
        Catalog.TableMetadata respTableMetadata = catalog.getTable("FirstTable");
        assertEquals(respTableMetadata.getName(), tableMetadata.getName());

        respTableMetadata = catalog.getTable(tableMetadata.getOid());
        assertEquals(respTableMetadata.getName(), tableMetadata.getName());

        respTableMetadata = catalog.getTable(tableMetadata.getOid());
        assertEquals(respTableMetadata.getName(), tableMetadata.getName());


        Catalog.TableMetadata tableMetadata2 = catalog.createTable("SecondTable", s);
        Catalog.TableMetadata respTableMetadata2 = catalog.getTable("SecondTable");
        assertEquals(respTableMetadata2.getName(), tableMetadata2.getName());

        respTableMetadata2 = catalog.getTable(tableMetadata2.getOid());
        assertEquals(respTableMetadata2.getName(), tableMetadata2.getName());

        respTableMetadata2 = catalog.getTable(tableMetadata2.getOid());
        assertEquals(respTableMetadata2.getName(), tableMetadata2.getName());

    }


    @org.junit.jupiter.api.Test
    public void testTableMap(){

        List<Column> cols = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("IsAdult", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s = new Schema(cols);
        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");

        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager);

        Catalog catalog = new Catalog(bufferPoolManager);

        Catalog.TableMetadata tableMetadata = catalog.createTable("FirstTable", s);
        Catalog.TableMetadata tableMetadata2 = catalog.createTable("SecondTable", s);

        Map<String, Integer> names = catalog.getNames();

        assertEquals((Integer) names.get("FirstTable"), (Integer) tableMetadata.getOid());
        assertEquals((Integer) names.get("SecondTable"), (Integer) tableMetadata2.getOid());

        Map<Integer, Catalog.TableMetadata> tables = catalog.getTables();

        assertEquals(tables.get(tableMetadata.getOid()).getName(), "FirstTable");
        assertEquals(tables.get(tableMetadata2.getOid()).getName(), "SecondTable");
    }
}
