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
    public ArrayList<Heapfile> bufferFiles;
    public ArrayList<String> bufferFilesNames;
    public ArrayList<String> pointerFileNames;
    public ArrayList<BTreeFile> pointerFiles;
    public BTreeFile util_index = null;
    public Heapfile heap_Files=null;
    public String index_Utility;
    private AttrType[] attribute_Type;
    private FldSpec[] project_List;
    private CondExpr[] cond_expr;
    MapIndexScan index_scan;
    private int add_Type;
    short[] short_str = new short[]{Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};

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
        bufferFiles = new ArrayList<>(6);
        pointerFiles = new ArrayList<>(6);

        bufferFilesNames = new ArrayList<>(6);
        pointerFileNames = new ArrayList<>(6);
        this.name = name;
        this.type = type;
        // Making 1 as the default index type.
        boolean insert = (type ==1);

        // Adding these so that we can have index of the array to the index type as the same number.
        bufferFiles.add(null);
        pointerFiles.add(null);
        bufferFilesNames.add("");
        pointerFileNames.add("");
        // 5 files for the 5 index types.
        for(int i = 1; i <= 5; i++){
            bufferFilesNames.add(name + "_" + i);
            pointerFileNames.add(name + "_index_" + i);
            bufferFiles.add(new Heapfile(bufferFilesNames.get(i)));
            pointerFiles.add(this.initIndex(pointerFileNames.get(i), i));
        }

        //  index_Utility is to specify the name of the index file that will be created for the B-Tree index.
        index_Utility = name + "_" + "index_Utility";

        if(insert){
            //For multiple batch insert

            initIndexUtil();
            indexUtilInit();

            try {
                // To ensure that the index is empty before inserting new data.
                allDeleteIndex(util_index);
                for(int i=2; i<=5; i++) {
                    allDeleteIndex(pointerFiles.get(i));
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Exception occurred while destroying all nodes of index files");
            }
        }
        initialize_condExpr();
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
            heap_Files.deleteFileMap();
            util_index.destroyFile();
        }

        catch(Exception e)
        {
            System.exit(1);
        }
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
            totalMapCount += bufferFiles.get(i).getRecCntMap();
        }
        return totalMapCount;
    }

    public int getRowCnt()  throws Exception{
        return getCount(3);
    }

    public int getColumnCnt()  throws Exception{
        return getCount(4);
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
        this.add_Type = type;
        MID mid = bufferFiles.get(type).insertRecordMap(map.getMapByteArray());
        return mid;
    }


    public Stream openStream(String bigTableName, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        Stream stream = new Stream(bigTableName, orderType, rowFilter, columnFilter, valueFilter, numBuf);
        return stream;
    }



    public void initialize_condExpr(){
        attribute_Type = new AttrType[4];
        attribute_Type[0] = new AttrType(AttrType.attrString);
        attribute_Type[1] = new AttrType(AttrType.attrString);
        attribute_Type[2] = new AttrType(AttrType.attrInteger);
        attribute_Type[3] = new AttrType(AttrType.attrString);
        project_List = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        project_List[0] = new FldSpec(rel, 0);
        project_List[1] = new FldSpec(rel, 1);
        project_List[2] = new FldSpec(rel, 2);
        project_List[3] = new FldSpec(rel, 3);

        cond_expr = new CondExpr[3];
        cond_expr[0] = new CondExpr();
        cond_expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        cond_expr[0].type1 = new AttrType(AttrType.attrSymbol);
        cond_expr[0].type2 = new AttrType(AttrType.attrString);
        cond_expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        cond_expr[0].operand2.string = "";
        cond_expr[0].next = null;
        cond_expr[1] = new CondExpr();
        cond_expr[1].op = new AttrOperator(AttrOperator.aopEQ);
        cond_expr[1].type1 = new AttrType(AttrType.attrSymbol);
        cond_expr[1].type2 = new AttrType(AttrType.attrString);
        cond_expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        cond_expr[1].operand2.string = "";
        cond_expr[1].next = null;
        cond_expr[2] = null;
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
    public Heapfile getbufferFile(int i) {
        return bufferFiles.get(i);
    }

    /**
     * Returns the name of the heap file for the provided type.
     * @param i Index type
     * @return
     */
    public String getbufferFileName(int i){
        return bufferFilesNames.get(i);
    }

    public void initIndexUtil() throws IOException,
            ConstructPageException,
            GetFileEntryException,
            AddFileEntryException{
        pointerFiles.add(null);
        BTreeFile _index = null;
        for(int i = 1; i <= 5; i++){
            _index = initIndex(pointerFileNames.get(i), i);
            pointerFiles.add(_index);
        }
    }


    public void allDeleteIndex(BTreeFile index) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException, ScanIteratorException, ScanDeleteException {
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
    public BTreeFile initIndex(String indexName1, int type) throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException{
        BTreeFile temp_Index=null;
        switch(type){
            case 1:
                break;
            case 2:
                // Index type 2
                temp_Index = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                // Index type 3
                temp_Index = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                // Index type 4
                temp_Index = new BTreeFile(indexName1, AttrType.attrString,
                        Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.NAIVE_DELETE);
                break;
            case 5:
                // Index type 4
                temp_Index = new BTreeFile(indexName1, AttrType.attrString,
                        Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.NAIVE_DELETE);
                break;
        }
        return temp_Index;
    }

    public void indexUtilInit(){
        try {
            util_index = new BTreeFile(index_Utility, AttrType.attrString,
                    Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 20, DeleteFashion.NAIVE_DELETE);
        }catch(Exception ex){
            System.err.println("Error in creating utility index");
            ex.printStackTrace();
        }
    }

    public void addIndex(MID mid, Map map, int type) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException {
        switch (type) {
            case 1:
                break;
            case 2:
                pointerFiles.get(2).insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                pointerFiles.get(3).insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                pointerFiles.get(4).insert(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
                break;
            case 5:
                pointerFiles.get(5).insert(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
                break;
        }
    }


    public void addIndexUtil(MID mid, Map map, int heapFileIndex)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException{
        util_index.insert(new StringKey(map.getRowLabel() + map.getColumnLabel() + "%" + map.getTimeStamp() + "%" + heapFileIndex), mid);
    }



    public int getCount(int orderType) throws Exception{
        int numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4);
        Stream stream = new Stream(this.name, 1,  "*", "*", "*", numBuf);
        Map temp_map = stream.getNext();
        int count = 0;
        String temp = "\0";
        while(temp_map != null) {
            temp_map.setFldOffset(temp_map.getMapByteArray());
            if(orderType==3){
                if(!temp_map.getRowLabel().equals(temp)){
                    temp = temp_map.getRowLabel();
                    count++;
                }
            }else{
                if(!temp_map.getColumnLabel().equals(temp)){
                    temp = temp_map.getColumnLabel();
                    count++;
                }
            }
            temp_map = stream.getNext();
        }
        stream.closestream();
        return count;
    }



    public void buildUtilityIndex(){
        try{
            FileScanMap fscan;
            String heapFileName;
            for(int i = 1; i<= 5; i++){
                heapFileName = getbufferFileName(i);
                fscan = new FileScanMap(heapFileName, null, null, false);
                Pair mapPair;
                mapPair = fscan.get_next_mid();
                while(mapPair!=null){
                    addIndexUtil(mapPair.getMid(), mapPair.getMap(), i);
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
            if(this.add_Type != 1){
                FileScanMap fscan;
                String heapFileName;
                heapFileName = bufferFilesNames.get(this.add_Type);
                fscan = new FileScanMap(heapFileName, null, null, false);
                int sortType = 1;
                switch (this.add_Type) {
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

                getbufferFile(this.add_Type).deleteFileMap();
                bufferFiles.set(this.add_Type, new Heapfile(heapFileName));

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
                    getbufferFile(this.add_Type).insertRecordMap(map.getMapByteArray());
                }
                fscan.close();
                fileToDestroy.deleteFileMap();
            }
        } catch(Exception ex) {
            System.err.println("Exception caused while creating sorted bufferFiles.");
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

        index_scan = new MapIndexScan(new IndexType(IndexType.B_Index), this.getbufferFileName(1), index_Utility, attribute_Type, short_str, 4, 4, project_List, null, null, 1, true);
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
                bufferFiles.get(duplicateMaps.get(0).getHeapFileIndex()).deleteRecordMap(mid);
                duplicateMaps.remove(0);
            }
            prevKey = curKey;
            curMapPair = index_scan.get_next_mid();
        }
        index_scan.close();
        util_index.close();

        if(duplicateMaps.size() == 4){
            mid = duplicateMaps.get(0).getMid();
            bufferFiles.get(duplicateMaps.get(0).getHeapFileIndex()).deleteRecordMap(mid);
            duplicateMaps.remove(0);
        }
    }

    public void insertIntoMainIndex(){
        FileScanMap fscan;
        for(int i = 2; i <= 5; i++){
            try{
                pointerFiles.set(i, initIndex(pointerFileNames.get(i), i));
                fscan = new FileScanMap(bufferFilesNames.get(i), null, null, false);
                Pair mapPair;
                mapPair = fscan.get_next_mid();
                while(mapPair!=null){
                    addIndex(mapPair.getMid(), mapPair.getMap(), i);
                    mapPair = fscan.get_next_mid();
                }
                fscan.close();
                pointerFiles.get(i).close();
            }catch(Exception ex){
                System.err.println("Exception caused in creating BTree Index for storage index type: " + i);
                ex.printStackTrace();
            }
        }
    }
}