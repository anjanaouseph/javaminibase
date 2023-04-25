package diskmgr;

public class PCounter {
    public static int rCounter;
    public static int wCounter;

    public static void initialize() {
        rCounter =0;
        wCounter =0;
    }

    public static void readIncrement() {
        rCounter++;
    }

    public static void writeIncrement() {
        wCounter++;
    }

    public static int getRCounter(){ return rCounter; }

    public static int getWCounter(){
        return wCounter;
    }
}
