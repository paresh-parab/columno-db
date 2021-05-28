package test.buffer;

import main.buffer.LRUReplacer;

import static main.common.Constants.INVALID_PAGE_ID;
import static org.junit.jupiter.api.Assertions.*;

public class LRUReplacerTest
{
    @org.junit.jupiter.api.Test
    void LRUTest1()
    {
        LRUReplacer<Integer> lruReplacer = new LRUReplacer<>();

        // push element into replacer
        lruReplacer.insert(1);
        lruReplacer.insert(2);
        lruReplacer.insert(3);
        lruReplacer.insert(4);
        lruReplacer.insert(5);
        lruReplacer.insert(6);
        lruReplacer.insert(1);

        assertEquals(6, lruReplacer.size());

        // pop element from replacer
        assertEquals(2, lruReplacer.victim());

        assertEquals(3, lruReplacer.victim());

        assertEquals(4, lruReplacer.victim());

        // remove element from replacer
        lruReplacer.erase(4);
        lruReplacer.erase(6);
        assertEquals(2, lruReplacer.size());

        // pop element from replacer after removal
        assertEquals(5, lruReplacer.victim());
        assertEquals(1, lruReplacer.victim());
    }

    @org.junit.jupiter.api.Test
    void LRUTest2()
    {
        LRUReplacer<Integer> lruReplacer = new LRUReplacer<>();

        assertNull(lruReplacer.victim());

        lruReplacer.insert(0);
        assertEquals(1, lruReplacer.size());

        assertEquals(0, lruReplacer.victim());
        assertNotEquals(0, lruReplacer.victim());

        assertFalse(lruReplacer.erase(0));
        assertEquals(0, lruReplacer.size());

        lruReplacer.insert(1);
        lruReplacer.insert(1);
        lruReplacer.insert(2);
        lruReplacer.insert(2);
        lruReplacer.insert(1);

        assertEquals(2, lruReplacer.size());

        assertEquals(2, lruReplacer.victim());
    }

    @org.junit.jupiter.api.Test
    void LRUTest3()
    {
        LRUReplacer<Integer> lruReplacer = new LRUReplacer<>();

        // push element into replacer
        for (int i = 0; i < 100; ++i) lruReplacer.insert(i);
        assertEquals(100, lruReplacer.size());

        // reverse then insert again
        for (int i = 0; i < 100; ++i) lruReplacer.insert(99 - i);

        // erase 50 element from the tail
        for (int i = 0; i < 50; ++i) assertTrue(lruReplacer.erase(i));

        // check left
        for (int i = 99; i >= 50; --i) {
            assertEquals(i, lruReplacer.victim());
        }
    }

    @org.junit.jupiter.api.Test
    void LRUTest4()
    {
        LRUReplacer<Integer> lruReplacer = new LRUReplacer<>(7);

        // Scenario: unpin six elements, i.e. add them to the replacer.
        lruReplacer.unpin(1);
        lruReplacer.unpin(2);
        lruReplacer.unpin(3);
        lruReplacer.unpin(4);
        lruReplacer.unpin(5);
        lruReplacer.unpin(6);
        lruReplacer.unpin(1);

        assertEquals(6, lruReplacer.size());

        // Scenario: get three victims from the lru.
        int value = -1;
        assertEquals(1, lruReplacer.victim());
        assertEquals(2, lruReplacer.victim());
        assertEquals(3, lruReplacer.victim());

        // Scenario: pin elements in the replacer.
        // Note that 3 has already been victimized, so pinning 3 should have no effect.
        lruReplacer.pin(3);
        lruReplacer.pin(4);
        assertEquals(2, lruReplacer.size());

        // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
        lruReplacer.unpin(4);

        // Scenario: continue looking for victims. We expect these victims.
        assertEquals(5, lruReplacer.victim());
        assertEquals(6, lruReplacer.victim());
        assertEquals(4, lruReplacer.victim());
    }
}
