package main;


import main.demo.RowStoreDemo;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        int choice = -1;
        do {
            System.out.println("Select 1 for row store");
            System.out.println("Select 2 for column store");
            System.out.println("Select 3 for auto mode");
            Scanner scanner = new Scanner(System.in);
            System.out.println("Make selection...");
            choice = Integer.parseInt(scanner.nextLine());  // Read user input
            switch (choice){
                case 1:
                    new RowStoreDemo().demo();
                    break;
                case 2:
                    break;
                case 3:
                    break;
                default: break;
            }
        }while(choice <= 3 && choice > 0);
    }
}
