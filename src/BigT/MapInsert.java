package BigT;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.UnknownKeyTypeException;

import java.io.IOException;

public class MapInsert {

    public MapInsert(String rowLabel, String colLabel, String value, int timestamp, int type, String bigTableName, int numbuf) throws IteratorException, HashEntryNotFoundException, ConstructPageException, GetFileEntryException, PinPageException, InvalidFrameNumberException, IOException, UnpinPageException, FreePageException, AddFileEntryException, HFDiskMgrException, HFException, HFBufMgrException, PageUnpinnedException, DeleteFileEntryException, ReplacerException, InvalidTupleSizeException, SpaceNotAvailableException, FieldNumberOutOfBoundException, InvalidSlotNumberException, DeleteRecException, KeyTooLongException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, KeyNotMatchException, NodeNotMatchException, LeafInsertRecException, IndexSearchException, LeafRedistributeException, RecordNotFoundException, InsertRecException, IndexException, UnknownKeyTypeException, DeleteFashionException, UnknownIndexTypeException, RedistributeException, InvalidTypeException, IndexFullDeleteException {
        if (SystemDefs.JavabaseDB == null) {
            // Initialize the data base.
            String dbpath = "/tmp/"+ bigTableName + "_" + type  + ".minibase-db";
            SystemDefs sysdef = new SystemDefs( dbpath, 1000000, numbuf*5, "Clock" );
        }

        // Calling the constructor with the data.
        // Since we retrieve/create the heap files with a standard name. If the table was already created, the right file would be fetched.
        bigt table = new bigt(bigTableName, type);

        // insert the rows into the table
        Map map = new Map();
        map.setDefaultHdr();

        map.setRowLabel(rowLabel);
        map.setColumnLabel(colLabel);
        map.setTimeStamp(timestamp);
        map.setValue(value);

        MID mid = table.insertMap(map, type);

        if (type > 1)
            table.insertIndex(mid, map, type);

        table.insertIndex(mid, map, 0);

        int noDuplicateRecordCount = table.deleteDuplicateRecords();

        // Stats
        System.out.println("TOTAL RECORDS READ FROM THE FILE : " + 1);
        System.out.println("TOTAL NON DUPLICATE RECORDS : " + noDuplicateRecordCount);
        System.out.println("READ COUNT : " + PCounter.rCounter);
        System.out.println("WRITE COUNT : " + PCounter.wCounter);
    }
}
