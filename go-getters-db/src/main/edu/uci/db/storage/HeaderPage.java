package main.edu.uci.db.storage;

public class HeaderPage extends Page {

    public boolean insertRecord(String name, int rootID) {
        //assert (name.length() < 32);
        //assert (rootID > INVALID_PAGE_ID);

        int recordCount = getRecordCount();
        // check for duplicate name
        if (findRecord(name) != -1)
            return false;

        addEntry(name, rootID, recordCount);
        setRecordCount(recordCount + 1);
        return true;
    }

    public boolean deleteRecord(String name) {
        int recordNum = getRecordCount();
        //assert (recordNum > 0);

        int index = findRecord(name);
        // record does not exsit
        if (index == -1)
            return false;
        removeEntry(index);
        setRecordCount(recordNum - 1);
        return true;
    }

    public boolean updateRecord(String name, int rootID) {
        assert (name.length() < 32);

        int index = findRecord(name);
        // record does not exsit
        if (index == -1)
            return false;
        getData().get(index).setRootID(rootID);
        return true;
    }

    public int getRecordCount() {
        return getCount();
    }

    public void setRecordCount(int recordCount) {
        setCount(recordCount);
    }

    public int findRecord(String name) {
        int recordNum = getRecordCount();

        for (int i = 0; i < recordNum; i++) {
            if(name.equals(getData().get(i).getName()))
                return i;
        }
        return -1;
    }

    public boolean getRootID(String name, Ref<Integer> rootID) {
        assert (name.length() < 32);

        int index = findRecord(name);
        // record does not exsit
        if (index == -1)
            return false;
        rootID.setValue(getData().get(index).getRootID());
        return true;
    }

    public void Init() {
        setRecordCount(0);
    }
}