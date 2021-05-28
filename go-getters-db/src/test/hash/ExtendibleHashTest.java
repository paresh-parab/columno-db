package test.hash;
import main.hash.ExtendibleHash;

import static org.junit.jupiter.api.Assertions.*;

public class ExtendibleHashTest
{
    @org.junit.jupiter.api.Test
    void ExtendibleHashTest1()
    {
        ExtendibleHash<Integer, String> ehash = new ExtendibleHash<>(2);
        ehash.insert(1, "a");
        ehash.insert(2, "b");
        ehash.insert(3, "c");
        ehash.insert(4, "d");
        ehash.insert(5, "e");
        ehash.insert(6, "f");
        ehash.insert(7, "g");
        ehash.insert(8, "h");
        ehash.insert(9, "i");

        assertEquals(2, ehash.getLocalDepth(0));
        assertEquals(3, ehash.getLocalDepth(1));
        assertEquals(2, ehash.getLocalDepth(2));
        assertEquals(2, ehash.getLocalDepth(3));


        // find test
        String result = null;

        result = ehash.find(9, result);
        assertEquals("i", result);

        result = ehash.find(8, result);
        assertEquals("h", result);

        result = ehash.find(2, result);
        assertEquals("b", result);

        assertNull(ehash.find(10, result));

        // delete test
        assertTrue(ehash.remove(8));
        assertTrue(ehash.remove(4));
        assertTrue(ehash.remove(1));
        assertFalse(ehash.remove(20));
    }
}
