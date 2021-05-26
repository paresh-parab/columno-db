package test.hash;
import main.hash.ExtendibleHash;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExtendibleHashTest
{
    @org.junit.jupiter.api.Test
    void insertValue()
    {
        ExtendibleHash<Integer, String> ehash = new ExtendibleHash<>(2);
        ehash.insert(1, "a");
        ehash.insert(2, "b");
        ehash.insert(3, "c");
//        ehash.insert(4, "d");
//        ehash.insert(5, "e");
//        ehash.insert(6, "f");
//        ehash.insert(7, "g");
//        ehash.insert(8, "h");
//        ehash.insert(9, "i");
//
//
//        assertEquals(2, ehash.getLocalDepth(0));
//        assertEquals(3, ehash.getLocalDepth(1));
//        assertEquals(2, ehash.getLocalDepth(2));
//        assertEquals(2, ehash.getLocalDepth(3));
    }


//
//        // find test
//        std::string result;
//        test->Find(9, result);
//        EXPECT_EQ("i", result);
//        test->Find(8, result);
//        EXPECT_EQ("h", result);
//        test->Find(2, result);
//        EXPECT_EQ("b", result);
//        EXPECT_EQ(0, test->Find(10, result));
//
//        // delete test
//        EXPECT_EQ(1, test->Remove(8));
//        EXPECT_EQ(1, test->Remove(4));
//        EXPECT_EQ(1, test->Remove(1));
//        EXPECT_EQ(0, test->Remove(20));
//
//        delete test;

}
