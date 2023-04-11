package BigT;

import java.io.*;
import java.util.ArrayList;

import bufmgr.*;
import global.*;
import btree.*;
import heap.*;
import iterator.*;
import index.*;
import java.util.*;

public class bigt {
    private String name;
    private int type;
    public ArrayList<Heapfile> heapFiles;
    public ArrayList<String> heapFileNames;
    public ArrayList<String> indexFileNames;
    public ArrayList<BTreeFile> indexFiles;
    public BTreeFile utilityIndex = null;
    public Heapfile hf=null;
    public String indexUtil;
    private AttrType[] attrType;
    private FldSpec[] projlist;
    private CondExpr[] expr;
    MapIndexScan index_scan;
    private int insertType;
    short[] res_str_sizes = new short[]{Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};

    /**
     * Constructor
     * @param name Name of the big table
     * @param type Index type
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws IOException
     * @throws GetFileEntryException
     * @throws ConstructPageException
     * @throws AddFileEntryException
     * @throws btree.IteratorException
     * @throws btree.UnpinPageException
     * @throws btree.FreePageException
     * @throws btree.DeleteFileEntryException
     * @throws btree.PinPageException
     * @throws PageUnpinnedException
     * @throws InvalidFrameNumberException
     * @throws HashEntryNotFoundException
     * @throws ReplacerException
     */
    public bigt(String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException, btree.IteratorException,
            btree.UnpinPageException, btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException,
            PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        String fileName = "";
        heapFiles = new ArrayList<>(6);
        indexFiles = new ArrayList<>(6);

        heapFileNames = new ArrayList<>(6);
        indexFileNames = new ArrayList<>(6);
        this.name = name;
        this.type = type;
        // Making 1 as the default index type.
        boolean insert = (type ==1);

        // Adding these so that we can have index of the array to the index type as the same number.
        heapFiles.add(null);
        indexFiles.add(null);
        heapFileNames.add("");
        indexFileNames.add("");
        // 5 files for the 5 index types.
        for(int i = 1; i <= 5; i++){
            heapFileNames.add(name + "_" + i);
            indexFileNames.add(name + "_index_" + i);
            heapFiles.add(new Heapfile(heapFileNames.get(i)));
            indexFiles.add(this.createIndex(indexFileNames.get(i), i));
        }

        //  indexUtil is to specify the name of the index file that will be created for the B-Tree index.
        indexUtil = name + "_" + "indexUtil";

        if(insert){
            //For multiple batch insert

            indexCreateUtil();
            createIndexUtil();

            try {
                // To ensure that the index is empty before inserting new data.
                deleteAllNodesInIndex(utilityIndex);
                for(int i=2; i<=5; i++) {
                    deleteAllNodesInIndex(indexFiles.get(i));
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Exception occurred while destroying all nodes of index files");
            }
        }
        initExprs();
    }

    public void initExprs(){
        attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 0);
        projlist[1] = new FldSpec(rel, 1);
        projlist[2] = new FldSpec(rel, 2);
        projlist[3] = new FldSpec(rel, 3);

        expr = new CondExpr[3];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        expr[0].operand2.string = "";
        expr[0].next = null;
        expr[1] = new CondExpr();
        expr[1].op = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[1].operand2.string = "";
        expr[1].next = null;
        expr[2] = null;
    }

    /**
     * Returns the name of this table
     * @return
     */
    public String getName(){
        return name;
    }

    /**
     * Returns the heap file for the corresponding index type.
     * @param i index type
     * @return
     */
    public Heapfile getHeapFile(int i) {
        return heapFiles.get(i);
    }

    /**
     * Returns the name of the heap file for the provided type.
     * @param i Index type
     * @return
     */
    public String getHeapFileName(int i){
        return heapFileNames.get(i);
    }

    public void indexCreateUtil() throws IOException,
            ConstructPageException,
            GetFileEntryException,
            AddFileEntryException{
        indexFiles.add(null);
        BTreeFile _index = null;
        for(int i = 1; i <= 5; i++){
            _index = createIndex(indexFileNames.get(i), i);
            indexFiles.add(_index);
        }
    }


    public void deleteAllNodesInIndex(BTreeFile index) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException, ScanIteratorException, ScanDeleteException {
        BTFileScan scan = index.new_scan(null, null);
        boolean isScanComplete = false;
        while(!isScanComplete) {
            KeyDataEntry entry = scan.get_next();
            if(entry == null) {
                isScanComplete = true;
                break;
            }
            scan.delete_current();
        }
    }

    /**
     *
     * @param indexName1
     * @param type
     * @return
     * @throws GetFileEntryException
     * @throws ConstructPageException
     * @throws IOException
     * @throws AddFileEntryException
     */
    public BTreeFile createIndex(String indexName1, int type) throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException{
        BTreeFile tempIndex=null;
        switch(type){

            case 1:
                // Index type 1 - No Index
//                tempIndex = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;

            case 2:
                // Index type 2 - Index on row label
                tempIndex = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                // Index type 3 - Index on column label
                tempIndex = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                // Index type 4 - Index on column and row label
                tempIndex = new BTreeFile(indexName1, AttrType.attrString,
                        Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.NAIVE_DELETE);
                break;
            case 5:
                // Index type 4 - Index on row label and value.
                tempIndex = new BTreeFile(indexName1, AttrType.attrString,
                        Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.NAIVE_DELETE);
                break;
        }
        return tempIndex;
    }

    public void createIndexUtil(){
        try {
            utilityIndex = new BTreeFile(indexUtil, AttrType.attrString,
                    Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 20, DeleteFashion.NAIVE_DELETE);
        }catch(Exception ex){
            System.err.println("Error in creating utility index");
            ex.printStackTrace();
        }
    }

    public void insertIndex(MID mid, Map map, int type) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException {
        switch (type) {
            case 1:
                // Index type 1 - No Index
//                indexFiles.get(1).insert(new StringKey(map.getValue()), mid);
                break;
            case 2:
                // Index type 2 - Index on row label
                indexFiles.get(2).insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                // Index type 3 - Index on column label
                indexFiles.get(3).insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                // Index type 4 - Index on column and row label
                indexFiles.get(4).insert(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
                break;
            case 5:
                // Index type 4 - Index on row label and value.
                indexFiles.get(5).insert(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
                break;
        }
    }

    public boolean removeIndex(MID mid, Map map, int type)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException {
        boolean status;
        if(type == 1){
            status = true;
        }else if(type == 2){
            status = indexFiles.get(2).Delete(new StringKey(map.getRowLabel()), mid);
        }else if(type == 3){
            status = indexFiles.get(3).Delete(new StringKey(map.getColumnLabel()), mid);
        }else if (type == 4){
            status = indexFiles.get(4).Delete(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
        }else{
            status = indexFiles.get(5).Delete(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
        }
        return status;
    }

    public void insertIndexUtil(MID mid, Map map, int heapFileIndex)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException{
        utilityIndex.insert(new StringKey(map.getRowLabel() + map.getColumnLabel() + "%" + map.getTimeStamp() + "%" + heapFileIndex), mid);
    }

    /**
     * Returns the count of the maps we maintained.
     * @return
     * @throws InvalidSlotNumberException
     * @throws InvalidTupleSizeException
     * @throws HFDiskMgrException
     * @throws HFBufMgrException
     * @throws IOException
     */
    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        int totalMapCount = 0;
        for(int i = 1; i <= 5; i++){
            totalMapCount += heapFiles.get(i).getRecCntMap();
        }
        return totalMapCount;
    }

    public int getRowCnt()  throws Exception{
        return getCount(3);
    }

    public int getColumnCnt()  throws Exception{
        return getCount(4);
    }

    public int getCount(int orderType) throws Exception{
        int numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4);
        Stream stream = new Stream(this.name, 1,  "*", "*", "*", numBuf);
        Map t = stream.getNext();
        int count = 0;
        String temp = "\0";
        while(t != null) {
            t.setFldOffset(t.getMapByteArray());
            if(orderType==3){
                if(!t.getRowLabel().equals(temp)){
                    temp = t.getRowLabel();
                    count++;
                }
            }else{
                if(!t.getColumnLabel().equals(temp)){
                    temp = t.getColumnLabel();
                    count++;
                }
            }
            t = stream.getNext();
        }
        stream.closestream();
        return count;
    }

    /**
     * type parameter helps to identify the specific heap file where the record should be inserted.
     * By passing the type parameter to the insertMap method, the method can access the correct heap file and perform the record insertion in the appropriate file.
     * @param map
     * @param type
     * @return
     * @throws HFDiskMgrException
     * @throws InvalidTupleSizeException
     * @throws HFException
     * @throws IOException
     * @throws FieldNumberOutOfBoundException
     * @throws InvalidSlotNumberException
     * @throws SpaceNotAvailableException
     * @throws HFBufMgrException
     */
    public MID insertMap(Map map, int type) throws HFDiskMgrException,
            InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException,
            InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        this.insertType = type;
        MID mid = heapFiles.get(type).insertRecordMap(map.getMapByteArray());
        return mid;
    }

    public void buildUtilityIndex(){
        try{
            FileScanMap fscan;
            String heapFileName;
            for(int i = 1; i<= 5; i++){
                heapFileName = getHeapFileName(i);
                fscan = new FileScanMap(heapFileName, null, null, false);
                Pair mapPair;
                mapPair = fscan.get_next_mid();
                while(mapPair!=null){
                    insertIndexUtil(mapPair.getMid(), mapPair.getMap(), i);
                    mapPair = fscan.get_next_mid();
                }
                fscan.close();
            }
        }catch(Exception ex){
            System.err.println("Exception caused in creating BTree Index");
            ex.printStackTrace();
        }
    }


    public void sortHeapFiles() {
        String tempFileName;
        try{
            if(this.insertType != 1){
                FileScanMap fscan;
                String heapFileName;
                heapFileName = heapFileNames.get(this.insertType);
                fscan = new FileScanMap(heapFileName, null, null, false);
                int sortType = 1;
                switch (this.insertType) {
                    case 3:
                    case 4:
                        sortType = 2;
                        break;
                    case 5:
                        sortType = 6;
                        break;
                    default:
                        sortType = 1;
                        break;
                }

                SortMap sortMap = new SortMap(null, null, null,
                        fscan, sortType, new MapOrder(MapOrder.Ascending), null,
                        (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4));

                Heapfile fileToDestroy = new Heapfile(null);
                tempFileName = fileToDestroy._fileName;
                boolean isScanComplete = false;
                MID resultMID = new MID();
                while (!isScanComplete) {
                    Map map = sortMap.get_next();
                    if (map == null) {
                        isScanComplete = true;
                        break;
                    }
                    map.setFldOffset(map.getMapByteArray());
                    fileToDestroy.insertRecordMap(map.getMapByteArray());
                }
                sortMap.close();

                getHeapFile(this.insertType).deleteFileMap();
                heapFiles.set(this.insertType, new Heapfile(heapFileName));

                fscan = new FileScanMap(tempFileName, null, null, false);
                isScanComplete = false;
                resultMID = new MID();
                while (!isScanComplete) {
                    Map map = fscan.get_next();
                    if (map == null) {
                        isScanComplete = true;
                        break;
                    }
                    map.setFldOffset(map.getMapByteArray());
                    getHeapFile(this.insertType).insertRecordMap(map.getMapByteArray());
                }
                fscan.close();
                fileToDestroy.deleteFileMap();
            }
        } catch(Exception ex) {
            System.err.println("Exception caused while creating sorted heapfiles.");
            ex.printStackTrace();
        }
    }

    public void deleteDuplicateRecords()
            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            UnknownKeyTypeException,
            java.io.IOException,
            InvalidSlotNumberException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

        index_scan = new MapIndexScan(new IndexType(IndexType.B_Index), this.getHeapFileName(1), indexUtil, attrType, res_str_sizes, 4, 4, projlist, null, null, 1, true);
        Pair previousMapPair = index_scan.get_next_mid();
        Pair curMapPair = index_scan.get_next_mid();

        String[] indexKeyTokens;

        String prevKey = previousMapPair.getIndexKey();
        String curKey = "";

        List<Pair> duplicateMaps = new ArrayList<>();
        indexKeyTokens = prevKey.split("%");
        previousMapPair  = new Pair(previousMapPair.getMap(), previousMapPair.getMid(), previousMapPair.getIndexKey(),
                Integer.parseInt(indexKeyTokens[indexKeyTokens.length-1]));
        duplicateMaps.add(previousMapPair);
        MID mid;
        Map map;
        while(curMapPair!=null){
            curKey = curMapPair.getIndexKey();
            indexKeyTokens = curKey.split("%");
            String curKeyString = curKey.substring(0, curKey.indexOf('%'));
            String prevKeyString = prevKey.substring(0, prevKey.indexOf('%'));

            curMapPair = new Pair(curMapPair.getMap(), curMapPair.getMid(), curMapPair.getIndexKey(),
                    Integer.parseInt(indexKeyTokens[indexKeyTokens.length-1]));

            if(prevKeyString.equals(curKeyString)){
                duplicateMaps.add(curMapPair);
            }else{
                duplicateMaps = new ArrayList<>();
                duplicateMaps.add(curMapPair);
            }
            if(duplicateMaps.size() == 4){
                duplicateMaps.sort(new Comparator<Pair>() {
                    @Override
                    public int compare(Pair o1, Pair o2) {
                        String o1String = o1.getIndexKey();
                        String o2String = o2.getIndexKey();

                        Integer o1Timestamp = Integer.parseInt(o1.getIndexKey().split("%")[1]);
                        Integer o2Timestamp = Integer.parseInt(o2.getIndexKey().split("%")[1]);
                        return o1Timestamp.compareTo(o2Timestamp);
                    }
                });
                mid = duplicateMaps.get(0).getMid();
                heapFiles.get(duplicateMaps.get(0).getHeapFileIndex()).deleteRecordMap(mid);
                duplicateMaps.remove(0);
            }
            prevKey = curKey;
            curMapPair = index_scan.get_next_mid();
        }
        index_scan.close();
        utilityIndex.close();

        if(duplicateMaps.size() == 4){
            mid = duplicateMaps.get(0).getMid();
            heapFiles.get(duplicateMaps.get(0).getHeapFileIndex()).deleteRecordMap(mid);
            duplicateMaps.remove(0);
        }
    }

    public Stream openStream(String bigTableName, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        Stream stream = new Stream(bigTableName, orderType, rowFilter, columnFilter, valueFilter, numBuf);
        return stream;
    }

    public void insertIntoMainIndex(){
        FileScanMap fscan;
        for(int i = 2; i <= 5; i++){
            try{
                indexFiles.set(i, createIndex(indexFileNames.get(i), i));
                fscan = new FileScanMap(heapFileNames.get(i), null, null, false);
                Pair mapPair;
                mapPair = fscan.get_next_mid();
                while(mapPair!=null){
                    insertIndex(mapPair.getMid(), mapPair.getMap(), i);
                    mapPair = fscan.get_next_mid();
                }
                fscan.close();
                indexFiles.get(i).close();
            }catch(Exception ex){
                System.err.println("Exception caused in creating BTree Index for storage index type: " + i);
                ex.printStackTrace();
            }

        }


    }

    /**
     * Deletes the Big table
     * @param name
     * @param type
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws InvalidSlotNumberException
     * @throws SpaceNotAvailableException
     * @throws InvalidTupleSizeException
     */
    public void deleteBigt(String name, int type)throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException
    {
        try {
            hf.deleteFileMap();
            utilityIndex.destroyFile();
        }

        catch(Exception e)
        {
            System.exit(1);
        }
    }

    public void createMapInsertIndex(int type){
        if(type == 1) {
            return;
        }
        FileScanMap fscan;
        try{
            deleteAllNodesInIndex(indexFiles.get(type));
        }catch(Exception e){
            System.err.println("Exception caused in deleting records in BTree index for storage type: " + type);
        }
        try{
            indexFiles.set(type, createIndex(indexFileNames.get(type), type));
            fscan = new FileScanMap(heapFileNames.get(type), null, null, false);
            Pair mapPair;
            mapPair = fscan.get_next_mid();
            while(mapPair!=null){
                insertIndex(mapPair.getMid(), mapPair.getMap(), type);
                mapPair = fscan.get_next_mid();
            }
            fscan.close();
            indexFiles.get(type).close();
        }catch(Exception ex){
            System.err.println("Exception caused in creating BTree Index for storage index type: " + type);
            ex.printStackTrace();
        }
    }


}