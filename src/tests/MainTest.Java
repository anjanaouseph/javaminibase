package tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

class MainTest {

    /**
     * The menu shown the user.
     */
    public static void menu(){

        System.out.println("------------------------ BigTable Tests --------------------------");
        System.out.println("Press 1 for Batch Insert");
        System.out.println("Press 2 for Query");
        System.out.println("Press 3 for MapInsert");
        System.out.println("Press 4 for RowJoin");
        System.out.println("Press 5 for RowSort");
        System.out.println("Press 6 for getCounts");
        System.out.println("Press 7 for other options");
        System.out.println("Press 8 to quit");
        System.out.println("------------------------ BigTable Tests --------------------------");
    }

    /**
     * Reads and returns the input choice of the user.
     * @return choice
     */
    public static int getChoice () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice;

        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            return -1;
        }
        catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static void main(String argv[]) {
        int choice = -1;
        // We keep on giving the choice to user until the user choose option 8, which corresponds to Quit.
        while(choice != 8) {
            menu();
            try {
                choice = getChoice();

                switch (choice) {
                    case 1:
                        //batch insert
                        // TODO: Make changes for batch insert.
                        break;
                    case 2:
                        // query
                        // TODO: Make changes for query.
                        break;
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        System.out.println("This feature is not yet implemented !");
                        System.out.println("Please choose again!!");
                }
            } catch (Exception exception) {
                exception.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!         Something is wrong                    !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            }
        }
    }
    }