package tests;

import BigT.BatchInsert;
import BigT.Map;
import BigT.Stream;
import BigT.bigt;

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
        // We keep on giving the choice to user until the user choose option 8, which corresponds to Quit.
        String[] input;
        while(choice != 8) {
            menu();
            try {
                choice = getChoice();

                switch (choice) {
                    case 1:
                        //batch insert
                        // TODO: Make changes for batch insert.
                        System.out.println("FORMAT : batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                        input = getInput();
                        if (input != null && input.length == 5) {
                            // TODO: May be check later and add conditons to check the values of input.
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
                            Stream stream = new bigt(input[1],Integer.parseInt(input[2])).openStream(input[1], Integer.parseInt(input[2]), input[3], input[4], input[5], Integer.parseInt(input[6])/4);

                            Map map = stream.getNext();
                            while (map != null) {
//                System.out.println("---" + map.getValue());
                                map = stream.getNext();
                            }
                        } else {
                            System.out.println("Improper input given. Try again !");
                        }
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