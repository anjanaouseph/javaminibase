package BigT;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.PCounter;
import global.IndexType;
import heap.*;
import index.IndexException;
import index.MapIndexScanBigT;
import index.UnknownIndexTypeException;
import iterator.UnknownKeyTypeException;

import java.io.IOException;

public class CreateIndex {
    public CreateIndex(String bigT, int type) throws IteratorException, HashEntryNotFoundException, ConstructPageException, GetFileEntryException, PinPageException, InvalidFrameNumberException, IOException, UnpinPageException, FreePageException, AddFileEntryException, HFDiskMgrException, HFException, HFBufMgrException, PageUnpinnedException, DeleteFileEntryException, ReplacerException, IndexException, InvalidTupleSizeException, UnknownIndexTypeException, InvalidTypeException, UnknownKeyTypeException, DeleteRecException, KeyTooLongException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, KeyNotMatchException, NodeNotMatchException, LeafInsertRecException, IndexSearchException {
        System.out.println("Big table name : " + bigT);

        bigt table = new bigt(bigT, type);

        table.indexFiles.get(type).destroyFile();

        table.indexFiles.add(type, table.createIndex(table.indexFileNames.get(type), type));

        MapIndexScanBigT index_scan = new MapIndexScanBigT(new IndexType(IndexType.B_Index), table, table.indexFileNames.get(0), table.attrType, table.res_str_sizes, 4, 4, table.projlist, null, null, 1, false);

        Pair readingPair = index_scan.get_next_mid();

        while (readingPair != null) {
            table.insertIndex(readingPair.mid, readingPair.map, type);
            readingPair = index_scan.get_next_mid();
        }

        for (int i=2; i <=5; i++) {
            table.indexFiles.get(i).close();
        }

        System.out.println("READ COUNT : " + PCounter.rCounter);
        System.out.println("WRITE COUNT : " + PCounter.wCounter);
    }
}
