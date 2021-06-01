package test.storage.storage.page;

import main.storage.page.HeaderPage;
import main.common.Ref;

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
        assertTrue(p.deleteRecord("Adesh"));
        assertFalse(p.deleteRecord("Santhosh"));
    }

    @org.junit.jupiter.api.Test
    void updateRecord() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        assertTrue(p.updateRecord("Paresh", 3));
        assertFalse(p.updateRecord("Adesh", 3));
    }

    @org.junit.jupiter.api.Test
    void getRecordCount() {
        HeaderPage p = new HeaderPage();
        assertEquals(p.getRecordCount(), 0);
        p.insertRecord("Paresh",1);
        p.insertRecord("Adesh",2);
        assertEquals(p.getRecordCount(), 2);
        assertTrue(p.deleteRecord("Adesh"));
        assertEquals(p.getRecordCount(), 1);

    }



    @org.junit.jupiter.api.Test
    void getRootID() {
        HeaderPage p = new HeaderPage();
        p.insertRecord("Paresh",1);
        p.insertRecord("Adesh",2);
        Ref<Integer> pareshRoot = new Ref<>(-1);
        assertTrue(p.getRootID("Paresh", pareshRoot));
        assertEquals(pareshRoot.getValue(), 1);
        assertTrue(p.updateRecord("Paresh", 3));
        assertTrue(p.getRootID("Paresh", pareshRoot));
        assertEquals(pareshRoot.getValue(), 3);
        Ref<Integer> santhoshRoot = new Ref<>(-1);
        assertFalse(p.getRootID("Santhosh", santhoshRoot));
        assertEquals(santhoshRoot.getValue(), -1);
    }


}