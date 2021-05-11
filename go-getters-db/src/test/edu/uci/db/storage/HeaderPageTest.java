package test.edu.uci.db.storage;

import main.edu.uci.db.storage.HeaderPage;
import main.edu.uci.db.storage.Ref;

import static org.junit.jupiter.api.Assertions.*;

class HeaderPageTest {

    @org.junit.jupiter.api.Test
    void insertRecord() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        assertEquals(p.findRecord("Paresh"), 0);
        assertEquals(p.findRecord("Adesh"), -1);
    }

    @org.junit.jupiter.api.Test
    void findRecord() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        p.insertRecord("Adesh",2);
        assertEquals(p.findRecord("Paresh"), 0);
        assertEquals(p.findRecord("Adesh"), 1);
        assertEquals(p.findRecord("Santhosh"), -1);
    }

    @org.junit.jupiter.api.Test
    void deleteRecord() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        p.insertRecord("Adesh",2);
        assertEquals(p.deleteRecord("Adesh"), true);
        assertEquals(p.deleteRecord("Santhosh"), false);
    }

    @org.junit.jupiter.api.Test
    void updateRecord() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        assertEquals(p.updateRecord("Paresh",3), true);
        assertEquals(p.updateRecord("Adesh",3), false);
    }

    @org.junit.jupiter.api.Test
    void getRecordCount() {
        HeaderPage p = new HeaderPage();
        assertEquals(p.getRecordCount(), 0);
        p.insertRecord("Paresh",1);
        p.insertRecord("Adesh",2);
        assertEquals(p.getRecordCount(), 2);
        assertEquals(p.deleteRecord("Adesh"), true);
        assertEquals(p.getRecordCount(), 1);

    }



    @org.junit.jupiter.api.Test
    void getRootID() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        p.insertRecord("Adesh",2);
        Ref<Integer> pareshRoot = new Ref<>(-1);
        assertEquals(p.getRootID("Paresh", pareshRoot), true);
        assertEquals(pareshRoot.getValue(), 1);
        assertEquals(p.updateRecord("Paresh",3), true);
        assertEquals(p.getRootID("Paresh", pareshRoot), true);
        assertEquals(pareshRoot.getValue(), 3);
        Ref<Integer> santhoshRoot = new Ref<>(-1);
        assertEquals(p.getRootID("Santhosh", santhoshRoot), false);
        assertEquals(santhoshRoot.getValue(), -1);
    }


}