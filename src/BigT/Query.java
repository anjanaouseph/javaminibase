package BigT;

import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;

import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Query {
    public Query(String bigtName, int type, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {

        String bigTable = bigtName+"_"+type;

        System.out.println("Table name : " + bigTable);
        System.out.println("Order type : " + orderType);
        System.out.println("Number of buffers : " + numBuf);

        try {

            // Calling the constructor with the data

            bigt table = new bigt(bigtName, orderType);
            // TODO ask TA : When would the buffer be freed ?
            // Reading the data inserted

            Stream stream = table.openStream(bigtName, orderType, rowFilter, columnFilter, valueFilter, numBuf/4);

            int count = 0;
            Map map = stream.getNext();
            while(map != null) {
                if (map == null) {
                    break;
                }
                map.setFldOffset(map.getMapByteArray());
                map.print();
                count++;
                map = stream.getNext();
            }

            stream.closestream();

            System.out.println("RECORD COUNT : "+ count);
            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);
        }
        catch (Exception e) {
            System.out.println("An error occurred: " + e.getMessage());
    }


    }
}