package BigT;

import diskmgr.PCounter;
import heap.Heapfile;

public class GetCounts {

    public GetCounts(String tableName, int numBuf) {
        System.out.println("Table name : " + tableName);
        System.out.println("Number of buffers : " + numBuf);

        try {
            bigt table = new bigt(tableName);

            int numberOfMaps = 0;
            int distinctRowLabelCount = 0;
            int distinctColumnLabelCount = 0;

            // For 5 index types
            for (int i = 1; i <=5; i++) {
                // heap file
                Heapfile hf = table.getHeapFile(i);
                if (hf != null) {
                    numberOfMaps += hf.getRecCntMap();
                }
            }

            // We only have to do it once as these methods use a combined scan so, it covers all the heap files.
            distinctRowLabelCount = table.getRowLabelCount(numBuf);
            distinctColumnLabelCount = table.getColLabelCount(numBuf);

            System.out.println("Map count : " + numberOfMaps);
            System.out.println("Distinct Row Label : " + distinctRowLabelCount);
            System.out.println("Distinct Col Label : " + distinctColumnLabelCount);
            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);
        } catch (Exception exception) {
            System.out.println("Ran into exception while running getCount");
            exception.printStackTrace();
        }
    }
}
