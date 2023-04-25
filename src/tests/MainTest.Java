package tests;

import BigT.BatchInsert;
import BigT.GetCounts;
import BigT.Query;
import BigT.bigt;
import diskmgr.PCounter;
import iterator.MapUtils;

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

    /**
     * Reads the input for the choice from the user
     * @return String[]
     */
    public static String[] getInput() {
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        try {
            String inputFromUser = in.readLine();
            // The input cmd is seperated by space.
            return inputFromUser.split(" ");
        } catch (Exception exception) {
            System.out.println("Ran into an exception reading the input.");
            exception.printStackTrace();
        }

        return null;
    }

    public static void main(String argv[]) {
        int choice = -1;

        bigt big = null;
        int pages = 0;
        String replacement_policy = "Clock";

        // We keep on giving the choice to user until the user choose option 8, which corresponds to Quit.
        String[] input;
        while(choice != 8) {
            menu();
            try {
                choice = getChoice();

                switch (choice) {
                    case 1:
                        //batch insert
                        System.out.println("FORMAT : batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                        input = getInput();
                        if (input != null && input.length == 5) {
                            PCounter.initialize();//to set the read and write counters to 0
                            new BatchInsert(input[1], Integer.parseInt(input[2]), input[3], Integer.parseInt(input[4]));
                        } else {
                            System.out.println("Improper input given. Try again !");
                        }
                        break;
                    case 2:
                        // query

                        System.out.println("FORMAT : query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                        input = getInput();
                        if (input != null && input.length == 7) {
                            PCounter.initialize();//to set the read and write counters to 0
                            new Query(input[1],Integer.parseInt(input[2]),input[3],input[4],input[5],Integer.parseInt(input[6]));

                        } else {
                            System.out.println("Improper input given. Try again !");
                        }
                        break;
                    case 3:
                    //map insert
                        System.out.println("FORMAT : mapinsert RL CL VAL TS TYPE BIGTABLENAME NUMBUF");
                        input = getInput();
                        if (input != null && input.length == 8) {
                            PCounter.initialize();//to set the read and write counters to 0
                            new MapInsert(input[1], input[2], input[3], Integer.parseInt(input[4]), Integer.parseInt(input[5]), input[6], Integer.parseInt(input[7]));
                        } else {
                            System.out.println("Improper input given. Try again !");
                        }
                        break;

                    case 4: System.out.println("FORMAT : ROWJOIN BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER JOINTYPE NUMBUF");
                        input = getInput();
                        if (input != null && input.length == 7) {

                            MapUtils.rowJoinWrapper(Integer.parseInt(input[6]),input[1],input[2],input[3],input[4],input[5]);
                            } else {
                            System.out.println("Improper input given. Try again !");
                        }
                        break;

                    case 5:

                            System.out.println("FORMAT: rowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF");
                            input = getInput();
                        if (input != null && input.length == 5) {
                            PCounter.initialize();//to set the read and write counters to 0
                            RowSort rowSort = new RowSort(input[1], input[2], input[3], (int) ((Integer.parseInt(input[4]) * 3) / 4));
                            rowSort.run();
                        } else{
                            System.out.println("Improper input given. Try again !");
                        }
                        break;
                    case 6:
                        System.out.println("FORMAT : getCounts BIGTABLENAME NUMBUF");
                        input = getInput();
                        if (input != null && input.length == 3) {
                            PCounter.initialize();//to set the read and write counters to 0
                            new GetCounts(input[1], Integer.parseInt(input[2]));
                        } else {
                            System.out.println("Improper input given. Try again !");
                        }
                        break;
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
