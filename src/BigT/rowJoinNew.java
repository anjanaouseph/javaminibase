package BigT;
import global.MID;
import global.MapOrder;
import heap.Heapfile;
import iterator.*;
import java.util.ArrayList;
import java.util.List;

public class rowJoinNew {
    private String columnName;
    private int amtOfMem;
    private bigt rightBigT, resultantBigT;
    private Stream leftStream, rightStream;
    private Heapfile leftHeapFile;
    private Heapfile rightHeapFile;
    private String LEFT_HEAP = "leftTempHeap";
    private String RIGHT_HEAP = "rightTempHeap";
    private MapOrder sortOrder = new MapOrder(MapOrder.Ascending);
    private SortMerge sm = null;
    private FileScanMap leftIterator, rightIterator;
    private String outBigTName;
    private String rightBigTName;
    private String leftName;

    public rowJoinNew(int amt_of_mem, Stream leftStream, String RightBigTName, String ColumnName, String outBigTName, String leftName, String JoinType)  throws Exception {
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

        SortMergeJoin();

    }


    public void SortMergeJoin() throws Exception {

        bigt table = new bigt(this.outBigTName, 1);

        while (leftStream.getNext() != null) {
            Map innerRow = leftStream.getNext();

            while (rightStream.getNext() != null) {
                Map outerRow = rightStream.getNext();

                if (innerRow.getValue().equals(outerRow.getValue()) && innerRow.getTimeStamp() != innerRow.getTimeStamp()) {
                    Map map1 = new Map();
                    map1.setDefaultHdr();
                    map1.setRowLabel(innerRow.getRowLabel() + ":" + outerRow.getRowLabel());
                    map1.setColumnLabel(innerRow.getColumnLabel());
                    map1.setTimeStamp(innerRow.getTimeStamp());
                    map1.setValue(innerRow.getValue());

                    Map map2 = new Map();
                    map2.setDefaultHdr();
                    map2.setRowLabel(innerRow.getRowLabel() + ":" + outerRow.getRowLabel());
                    map2.setColumnLabel(innerRow.getColumnLabel());
                    map2.setTimeStamp(outerRow.getTimeStamp());
                    map2.setValue(innerRow.getValue());

                    MID mid1 = table.insertMap(map1, 1);
                    table.insertIndex(mid1, map1, 0);
                    MID mid2 = table.insertMap(map2, 1);
                    table.insertIndex(mid2, map2, 0);

                } else if (innerRow.getValue().equals(outerRow.getValue()) && innerRow.getTimeStamp() == innerRow.getTimeStamp()) {

                    Map map1 = new Map();
                    map1.setDefaultHdr();
                    map1.setRowLabel(innerRow.getRowLabel() + ":" + outerRow.getRowLabel());
                    map1.setColumnLabel(innerRow.getColumnLabel());
                    map1.setTimeStamp(innerRow.getTimeStamp());
                    map1.setValue(innerRow.getValue());

                    MID mid1 = table.insertMap(map1, 1);
                    table.insertIndex(mid1, map1, 0);

                }
            }
        }
        int noDuplicateRecordCount = table.deleteDuplicateRecords();
    }
}
