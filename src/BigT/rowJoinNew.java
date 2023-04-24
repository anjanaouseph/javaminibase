package BigT;

import diskmgr.PCounter;
import global.MID;
import heap.Heapfile;

import java.util.ArrayList;

public class rowJoinNew {
    private String columnName;
    private int amtOfMem;
    private bigt rightBigT, resultantBigT;
    private Stream leftStream, rightStream;
    private Heapfile leftHeapFile;
    private Heapfile rightHeapFile;
    private String LEFT_HEAP = "leftTempHeap";
    private String RIGHT_HEAP = "rightTempHeap";
    private String outBigTName;
    private String rightBigTName;
    private String leftName;

    private String JoinType;

    public rowJoinNew(int amt_of_mem, Stream leftStream, String RightBigTName, String ColumnName, String outBigTName, String leftName, String JoinType) throws Exception {

        this.JoinType = JoinType;
        this.columnName = ColumnName;
        this.amtOfMem = amt_of_mem;
        this.rightBigTName = RightBigTName;
        this.rightBigT = new bigt(RightBigTName);
        this.leftStream = leftStream;
        // Left stream should be filtered on column
        this.rightStream = this.rightBigT.openStream(rightBigTName, 1, "*", columnName, "*", amt_of_mem);
        this.leftHeapFile = new Heapfile(LEFT_HEAP);
        this.rightHeapFile = new Heapfile(RIGHT_HEAP);
        this.outBigTName = outBigTName;
        this.leftName = leftName;

        if(this.JoinType.equalsIgnoreCase("1")) {

            System.out.println("............You have chosen SortMergeJoin...............");
            System.out.println("............Implementing SortMergeJoin...............");
            SortMergeJoin();

        } else{
            System.out.println("............You have chosen CartesianJoin...............");
            System.out.println("............Implementing CartesianJoin...............");
            CartesianJoin();
        }

    }


    public void SortMergeJoin() throws Exception {



        if (leftStream.getNext() == null || rightStream.getNext() == null)//if no column label matches then either one will be emtpty
        {
            bigt table = new bigt(this.outBigTName, 1);//creates an empty table of type 1

        }
        else {
            try {
                bigt table = new bigt(this.outBigTName, 1);

                ArrayList<Map> outerRelation = new ArrayList<>();
                ArrayList<Map> innerRelation = new ArrayList<>();

                Map map3 = leftStream.getNext();
                while (map3 != null) {
                    Map newMap = new Map(map3); // Create a new map object with the same entries as map3
                    outerRelation.add(newMap); // Add the new map object to the list
                    map3 = leftStream.getNext(); // Get the next map from the stream
                }
                leftStream.closestream();

                Map map4 = rightStream.getNext();
                while (map4 != null) {
                    Map newMap = new Map(map4); // Create a new map object with the same entries as map3
                    innerRelation.add(newMap);
                    map4 = rightStream.getNext();
                }

                rightStream.closestream();

                for (Map outerRow : outerRelation) {

                    for (Map innerRow : innerRelation) {

                        if (outerRow.getValue().equals(innerRow.getValue()) && outerRow.getTimeStamp() != innerRow.getTimeStamp()) {
                            Map map1 = new Map();
                            map1.setDefaultHdr();
                            map1.setRowLabel(outerRow.getRowLabel() + ":" + innerRow.getRowLabel());
                            map1.setColumnLabel(outerRow.getColumnLabel());
                            map1.setTimeStamp(outerRow.getTimeStamp());
                            map1.setValue(outerRow.getValue());

                            Map map2 = new Map();
                            map2.setDefaultHdr();
                            map2.setRowLabel(outerRow.getRowLabel() + ":" + innerRow.getRowLabel());
                            map2.setColumnLabel(outerRow.getColumnLabel());
                            map2.setTimeStamp(innerRow.getTimeStamp());
                            map2.setValue(outerRow.getValue());

                            MID mid1 = table.insertMap(map1, 1);
                            table.insertIndex(mid1, map1, 0);
                            MID mid2 = table.insertMap(map2, 1);
                            table.insertIndex(mid2, map2, 0);

                        } else if (outerRow.getValue().equals(innerRow.getValue()) && outerRow.getTimeStamp() == innerRow.getTimeStamp()) {

                            Map map1 = new Map();
                            map1.setDefaultHdr();
                            map1.setRowLabel(outerRow.getRowLabel() + ":" + innerRow.getRowLabel());
                            map1.setColumnLabel(outerRow.getColumnLabel());
                            map1.setTimeStamp(outerRow.getTimeStamp());
                            map1.setValue(outerRow.getValue());

                            MID mid1 = table.insertMap(map1, 1);
                            table.insertIndex(mid1, map1, 0);

                        }
                    }
                }
                int noDuplicateRecordCount = table.deleteDuplicateRecords();

                System.out.println("TOTAL NON DUPLICATE RECORDS : " + noDuplicateRecordCount);
                System.out.println("READ COUNT : " + PCounter.rCounter);
                System.out.println("WRITE COUNT : " + PCounter.wCounter);
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }

    public void CartesianJoin() throws Exception {

        try {
            // Create a new big table to store the result
            bigt table = new bigt(this.outBigTName, 1);

            ArrayList<Map> outerRelation = new ArrayList<>();
            ArrayList<Map> innerRelation = new ArrayList<>();

            // Read all the maps from the left table into memory
            Map map1 = leftStream.getNext();
            while (map1 != null) {
                Map newMap = new Map(map1);
                outerRelation.add(newMap);
                map1 = leftStream.getNext();
            }
            leftStream.closestream();

            // Read all the maps from the right table into memory
            Map map2 = rightStream.getNext();
            while (map2 != null) {
                Map newMap = new Map(map2);
                innerRelation.add(newMap);
                map2 = rightStream.getNext();
            }
            rightStream.closestream();

            // Perform the Cartesian Join operation
            for (Map outerRow : outerRelation) {
                for (Map innerRow : innerRelation) {
                    Map map = new Map();
                    map.setDefaultHdr();
                    map.setRowLabel(outerRow.getRowLabel() + ":" + innerRow.getRowLabel());
                    map.setColumnLabel(outerRow.getColumnLabel());
                    map.setTimeStamp(outerRow.getTimeStamp());
                    map.setValue(outerRow.getValue() + "," + innerRow.getValue());

                    MID mid = table.insertMap(map, 1);
                    table.insertIndex(mid, map, 0);
                }
            }

            int recordCount = outerRelation.size() * innerRelation.size();
            System.out.println("TOTAL RECORDS : " + recordCount);
            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
