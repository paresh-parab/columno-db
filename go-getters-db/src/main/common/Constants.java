package main.common;

import java.awt.*;

public class Constants
{
    public static class Debug{
        public void info(String msg){
            System.out.println( "\u001B[32m" + msg+ "\u001B[0m");
        }
    }
    public static final int INVALID_PAGE_ID  = -1;
    public static final int PAGE_SIZE  = 512;
    public static final int HEADER_PAGE_ID = 0;
    public static final int BUCKET_SIZE = 100;
    public static final String COLUMN_SEP = "\t";
    public static final String LINE_SEP =  System.lineSeparator();
    public static final Debug DEBUGGER = new Debug();

}
