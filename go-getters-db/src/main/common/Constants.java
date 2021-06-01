package main.common;

public class Constants
{
    public static class Debug{
        public void info(String msg){
            System.out.println(msg);
        }
    }
    // representing an invalid page id
    public static final int INVALID_PAGE_ID  = -1;
    public static final int PAGE_SIZE  = 512;     // size of a data page in terms of number of recordss
    public static final int HEADER_PAGE_ID = 0;
    public static final int BUCKET_SIZE = 100;
    public static final String COLUMN_SEP = "\t";
    public static final String LINE_SEP =  System.lineSeparator();
    public static final Debug DEBUGGER = new Debug();

}
