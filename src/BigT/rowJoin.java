package BigT;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Heapfile;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class rowJoin {
    private String columnName;
    private int amtOfMem;
    private bigt rightBigT, resultantBigT;
    private Stream leftStream, rightStream;
    private Heapfile leftHeapFile;
    private Heapfile rightHeapFile;
    private String LEFT_HEAP = "leftTempHeap";
    private String RIGHT_HEAP = "rightTempHeap";
    private TupleOrder sortOrder = new TupleOrder(TupleOrder.Ascending);
    private SortMerge sm = null;
    private FileScan leftIterator, rightIterator;
    private String outBigTName;
    private String rightBigTName;
    private String leftName;

    public rowJoin(int amt_of_mem, Stream leftStream, String RightBigTName, String ColumnName, String outBigTName, String leftName, String JoinType)  throws Exception {
        this.columnName = ColumnName;
        this.amtOfMem = amt_of_mem;
        this.rightBigTName = RightBigTName;
        this.rightBigT = new bigt(RightBigTName);
        this.leftStream = leftStream;
        // Left stream should be filtered on column
        this.rightStream = this.rightBigT.openStream(rightBigTName, 1,"*", columnName, "*",amt_of_mem);
        this.leftHeapFile = new Heapfile(LEFT_HEAP);
        this.rightHeapFile = new Heapfile(RIGHT_HEAP);
        this.outBigTName = outBigTName;
        this.leftName = leftName;

        storeLeftColMatch();
        storeRightColMatch();
        SortMergeJoin();
        StoreJoinResult();
        cleanUp();
    }


    public void storeLeftColMatch() throws Exception {
        Map tempMap = this.leftStream.getNext();
        Map oldMap = new Map();
        oldMap.setHdr((short) 0,bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES); // what is numfields in map header ask Nagarjun?
        oldMap.mapCopy(tempMap);
        String tempRow = tempMap.getRowLabel();
//        System.out.println("Left Stream results => ");
        while (tempMap!= null) {
            if (!tempMap.getRowLabel().equals(tempRow)) {
//                oldMap.print();
                this.leftHeapFile.insertRecordMap(oldMap.getMapByteArray());
            }
            tempRow = tempMap.getRowLabel();
            oldMap.mapCopy(tempMap);
            tempMap = this.leftStream.getNext();
        }
//        oldMap.print();
        this.leftHeapFile.insertRecordMap(oldMap.getMapByteArray());
        this.leftStream.closestream();
    }


    public void storeRightColMatch() throws Exception {
        Map tempMap = this.rightStream.getNext();
        Map oldMap = new Map();
        oldMap.setHdr((short) 0,bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES);
        oldMap.mapCopy(tempMap);
        String tempRow = tempMap.getRowLabel();
//        System.out.println("Right Stream results => ");
        while (tempMap!= null) {
            if (!tempMap.getRowLabel().equals(tempRow)) {
//                oldMap.print();
                this.rightHeapFile.insertRecordMap(oldMap.getMapByteArray());
            }
            tempRow = tempMap.getRowLabel();
            oldMap.mapCopy(tempMap);
            tempMap = this.rightStream.getNext();
        }
//        oldMap.print();
        this.rightHeapFile.insertRecordMap(oldMap.getMapByteArray());

        this.rightStream.closestream();
    }

    public static Map getJoinMap(String rowKey, String columnKey, String value, Integer timestamp) throws IOException {

        Map map = new Map();
        try {
            map.setHdr((short) 0,bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map map1 = new Map(map.size());
        try {
            map1.setHdr((short) 0,bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES);
            map1.setRowLabel(rowKey);
            map1.setColumnLabel(columnKey);
            map1.setTimeStamp(timestamp);
            map1.setValue(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map1;
    }

    public void StoreJoinResult() throws Exception {
        Map tempMap = sm.getnext();
        while (tempMap != null) {
            storeToBigT(tempMap.getRowLabel(), tempMap.getColumnLabel());
            tempMap = sm.getnext();
        }
        sm.close();
    }

    public void SortMergeJoin() throws Exception {
        MapIterator leftIterator, rightIterator;

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        outFilter[1] = null;

        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        try {
            this.leftIterator = new FileScan(LEFT_HEAP, bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            this.rightIterator = new FileScan(RIGHT_HEAP, bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            this.sm = new SortMerge(bigt.BIGT_ATTR_TYPES, 4, bigt.BIGT_STR_SIZES, bigt.BIGT_ATTR_TYPES,
                    4, bigt.BIGT_STR_SIZES, 3, 4, 3, 4, amtOfMem,
                    this.leftIterator, this.rightIterator, false, false, sortOrder, outFilter,
                    projection, 1);
        } catch (Exception e) {
            System.err.println(e);
        }
    }


    public void storeToBigT(String leftRowLabel, String rightRowLabel) throws Exception {
        // TODO: set self bigTName
        List<Map> joinedMaps = new ArrayList<>();
        String bigTName = this.leftName;
        String JOIN_BT_NAME = leftRowLabel + rightRowLabel;
        resultantBigT = new bigt(this.outBigTName, 1);
        Stream tempStream = new bigt(bigTName, 1).openStream(bigTName, 1, "*", "*","*",this.amtOfMem);
        Map tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (tempMap.getColumnLabel().equals(this.columnName) == true) {
                Map m2 = new Map();
                m2.setHdr((short) 0,bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES);
                m2.mapCopy(tempMap);
                joinedMaps.add(m2);
            } else {
                String rowLabel = leftRowLabel + ":" + rightRowLabel;
                String columnLabel = leftRowLabel + ":" + tempMap.getColumnLabel();
                String ValueLabel = tempMap.getValue();
                Integer timeStampVal = tempMap.getTimeStamp();

                Map tempMap2 = getJoinMap(rowLabel, columnLabel, ValueLabel, timeStampVal);
                if(tempMap2!=null) {
                    try {
                        resultantBigT.insertMap(tempMap2, 1);// check this
                    } catch (Exception e) {
                        System.out.println(columnLabel);
                        //e.printStackTrace();
                    }
                }
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closestream();


        tempStream = new bigt(rightBigTName).openStream(rightBigTName,1, rightRowLabel, "*", "*",this.amtOfMem);
        tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (tempMap.getColumnLabel().equals(this.columnName)) {
                Map m2 = new Map();
                m2.setHdr((short) 0,bigt.BIGT_ATTR_TYPES, bigt.BIGT_STR_SIZES);///ask nagarjun whats the first args
                m2.mapCopy(tempMap);
                joinedMaps.add(m2);
            } else {
                String rowLabel = leftRowLabel + ":" + rightRowLabel;
                String columnLabel = rightRowLabel + ":" + tempMap.getColumnLabel();
                String ValueLabel = tempMap.getValue();
                Integer timeStampVal = tempMap.getTimeStamp();

                Map tempMap2 = getJoinMap(rowLabel, columnLabel, ValueLabel, timeStampVal);
                if(tempMap2!=null) {
                    try {
                        resultantBigT.insertMap(tempMap2, 1);
                    } catch (Exception e) {
                        System.out.println(columnLabel);
                        //e.printStackTrace();
                    }
                }
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closestream();

        // Remove duplicates
        getFirstThree(joinedMaps);

        for (Map tempMap3 : joinedMaps) {
            String rowLabel = leftRowLabel + ":" + rightRowLabel;
            String columnLabel = tempMap3.getColumnLabel();
            String ValueLabel = tempMap3.getValue();
            Integer timeStampVal = tempMap3.getTimeStamp();

            Map tempMap4 = getJoinMap(rowLabel, columnLabel, ValueLabel, timeStampVal);
            resultantBigT.insertMap(tempMap4, 1);
        }
    }

    public void getFirstThree(List<Map> mapList) throws Exception {
        // TODO: add concatenate TS
//        for(Map map: mapList){
//            map.print();
//        }
        if (mapList.size() <= 3) {
            return;
        }

        do {
            int minIndex = 0;
            int minTimeStamp = mapList.get(0).getTimeStamp();
            for (int i = 1; i < mapList.size(); i++) {
                if (mapList.get(i).getTimeStamp() < minTimeStamp) {
                    minTimeStamp = mapList.get(i).getTimeStamp();
                    minIndex = i;
                }
            }

            mapList.remove(minIndex);
        } while (mapList.size() != 3);

    }

    public void cleanUp() throws Exception {
        try {
            if (resultantBigT != null) {
                resultantBigT.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ResultantBigT did not close");
        }

        this.leftHeapFile.deleteFileMap();
        this.rightHeapFile.deleteFileMap();
    }
}
