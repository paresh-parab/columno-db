package main;

import main.hash.ExtendibleHash;

public class Main
{

    public static void main(String[] args)
    {
        ExtendibleHash<Integer, String> ehash = new ExtendibleHash<>(2);
        ehash.insert(1, "a");
    }
}
