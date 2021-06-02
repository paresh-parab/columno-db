package main.demo;

import main.catalog.Schema;
import main.storage.table.Tuple;

import java.util.ArrayList;
import java.util.List;

public class Formatter {

    public static void printHorizontalLine(){
        for(int i=0; i<200; i++){
            System.out.print("_");
        }
        System.out.println();
    }

    public static void printColumns(List<String> list){
        StringBuilder res = new StringBuilder();
        res.append("|");
        for(String s: list){
            int len = s.length();
            int diff = 30 - len;
            int leftPad = diff/2;
            int rightPad = diff/2 + (diff%2 == 1 ? 1: 0 );
            res.append(" ".repeat(leftPad) + s + " ".repeat(rightPad) + "|");
        }
        System.out.println(res.toString());
    }

    public static void prettyPrintTable(List<Tuple> rows, Schema s){
        printHorizontalLine();
        printColumns(s.getColumns().stream().map(a->a.getName()).toList());
        printHorizontalLine();
        for(Tuple t: rows){
            printColumns(t.getContent().stream().map(a -> a.toString()).toList());
            printHorizontalLine();
        }

    }

    public static void prettyPrintTuple(Tuple t){
        printHorizontalLine();
        printColumns(t.getContent().stream().map(a -> a.toString()).toList());
        printHorizontalLine();
    }
}
