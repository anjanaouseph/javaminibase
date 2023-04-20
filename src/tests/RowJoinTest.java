//package tests;
//
//import BigT.*;
//import BigT.bigt;
//import BigT.rowJoin;
//import btree.AddFileEntryException;
//import btree.ConstructPageException;
//import btree.GetFileEntryException;
//import btree.PinPageException;
//import bufmgr.*;
//
//import global.SystemDefs;
//import heap.*;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//
//import static global.GlobalConst.NUMBUF;
//
//public class joinTest {
//
//    public static void batchInsert(String dataFile, String tableName, Integer type) throws Exception {
//        // Set the metadata name for the given DB. This is used to set the headers for the Maps
//        File file = new File("/tmp/" + tableName + "_metadata.txt");
//        FileWriter fileWriter = new FileWriter(file);
//        BufferedWriter bufferedWriter =
//                new BufferedWriter(fileWriter);
//        bufferedWriter.write(dataFile);
//        bufferedWriter.close();
//        Utils.batchInsert(dataFile, tableName, type, NUMBUF);
//    }
//
//    public static void getCount(String tableName) throws PageNotFoundException, PagePinnedException, PageUnpinnedException, HashOperationException, ReplacerException, BufMgrException, InvalidFrameNumberException, IOException, HashEntryNotFoundException, InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, ConstructPageException, AddFileEntryException, HFException, GetFileEntryException, PinPageException {
//        new SystemDefs(Utils.getDBPath(), Utils.NUM_PAGES, NUMBUF, "Clock");
//        bigt bigT = new bigt(tableName);
//        System.out.println("bigT.getMapCnt() = " + bigT.getMapCnt());
//        bigT.close();
//    }
//
//    public static void rowJoin(Integer type) throws Exception {
//        rowJoin rj;
//        String colName = "Zebra";
//        new SystemDefs(Utils.getDBPath(), Utils.NUM_PAGES, NUMBUF, "Clock");
//
//        Stream leftstream = new bigt("ganesh1").openStream("ganesh1", 1, "*", colName,"*",NUMBUF);
//        rj = new rowJoin(20, leftstream, "ganesh2", colName, "ash20", "ganesh1","1");
////        SystemDefs.JavabaseBM.setNumBuffers(0);
////        SystemDefs.JavabaseBM.flushAllPages();
////        SystemDefs.JavabaseDB.closeDB();
//        System.out.println("Query results => ");
//        Utils.query("ash20", 1, "*", "*", "*", NUMBUF);
//
//    }
//
//    public static void main(String[] args) throws Exception {
//
////        batchinsert DATAFILENAME TYPE BIGTABLENAME
//
////        // batch insert 1
//        Integer type = Integer.parseInt("1");
//
////        String dataFile = "/home/ganesh/Documents/Documents/DBMSI/phase3/test/test/ts1.csv";
//
//
//        String dataFile = "/Users/sumukhashwinkamath/Downloads/test/ts1.csv";
//        String tableName = "ganesh1";
//        batchInsert(dataFile, tableName, type);
//        getCount(tableName);
//
//////        // batch insert 2
//
////        dataFile = "/home/ganesh/Documents/Documents/DBMSI/phase3/test/test/ts2.csv";
//
//        dataFile = "/Users/sumukhashwinkamath/Downloads/test/ts2.csv";
//
//        tableName = "ganesh2";
//        batchInsert(dataFile, tableName, type);
//        getCount(tableName);
//
////        rowJoin(type);
//
//    }
//}
