package main.storage.page;

import main.common.Pair;
import main.common.Ref;

public class HeaderPage extends IndexPage<String, Integer> {

    public boolean insertRecord(String name, int rootID) {

        int recordCount = getRecordCount();
        // check for duplicate name
        if (findRecord(name) != -1)
            return false;

        getData().add(new Pair<>(name, rootID));
        setRecordCount(recordCount + 1);
        return true;
    }

    public boolean deleteRecord(String name) {
        int recordNum = getRecordCount();

        int index = findRecord(name);
        // record does not exsit
        if (index == -1)
            return false;
        getData().set(index, new Pair<>());
        setRecordCount(recordNum - 1);
        return true;
    }

    public boolean updateRecord(String name, int rootID) {
        int index = findRecord(name);
        // record does not exsit
        if (index == -1)
            return false;
        ((Pair<String, Integer>)getData().get(index)).setValue(rootID);
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
            if(name.equals(((Pair<String, Integer>)getData().get(i)).getKey()))
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
        rootID.setValue(((Pair<String, Integer>)getData().get(index)).getValue());
        return true;
    }

    public void Init() {
        setRecordCount(0);
    }
}