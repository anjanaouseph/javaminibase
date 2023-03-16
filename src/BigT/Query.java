package BigT;

import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Query {
    public Query(String bigtName, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {

        System.out.println("Index type : " + orderType);
        System.out.println("Table name : " +bigtName );
        System.out.println("Number of buffers : " + numBuf);


        // TODO: Check if the data file exists
        // TODO: Check if the table exists
        // TODO: Maybe a switch case with the index type

        try {

            String dbpath = "/tmp/batch-insert"+System.getProperty("user.name")+".minibase-db";
            SystemDefs sysdef = new SystemDefs( dbpath, 10000, numBuf/2, "Clock" );

            // Calling the constructor with the data
            bigt table = new bigt(bigtName, orderType);

            // Reading the data inserted
            Stream stream = table.openStream(bigtName, orderType, "*", "*", "*", numBuf/4);
            Map map = stream.getNext();
            while (map != null) {
//                System.out.println("---" + map.getValue());
                map = stream.getNext();
            }

            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}