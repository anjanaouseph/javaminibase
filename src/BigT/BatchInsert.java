package BigT;

public class BatchInsert {
    public BatchInsert(String datafile, int type,  String bigTable, int numbuf) {
        System.out.println("Starting to read from the data file : " + datafile);
        System.out.println("Index type : " + type);
        System.out.println("Table name : " + bigTable);
        System.out.println("Number of buffers : " + numbuf);
    }
}