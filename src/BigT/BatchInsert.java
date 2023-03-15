package BigT;

import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BatchInsert {
    public BatchInsert(String datafile, int type,  String bigTableName, int numbuf) {
        System.out.println("Starting to read from the data file : " + datafile);
        System.out.println("Index type : " + type);
        System.out.println("Table name : " + bigTableName);
        System.out.println("Number of buffers : " + numbuf);


        // TODO: Check if the data file exists
        // TODO: Check if the table exists
        // TODO: Maybe a switch case with the index type

        try {
            // TODO: Change the name of the data base.
            String dbpath = "/tmp/batch-insert"+System.getProperty("user.name")+".minibase-db";
            SystemDefs sysdef = new SystemDefs( dbpath, 10000, numbuf/2, "Clock" );

            // Calling the constructor with the data
            bigt table = new bigt(bigTableName, type);

            List<String> lines = Files.readAllLines(Paths.get(datafile));
            // There is no header
            List<String[]>  rows = lines.stream().map(line -> line.split(",")).collect(Collectors.toList());

            int recordNum = 0;
            for (String[] row : rows) {
                recordNum++;
                // reading each row
                if (row.length < 4 || row.length > 4) {
                    throw new Exception("Excepted row length 4.");
                }

                // insert the rows into the table
                Map map = new Map();
                map.setDefaultHdr();
                map.setRowLabel(row[0]);
                map.setColumnLabel(row[1]);
                map.setTimeStamp(Integer.parseInt(row[2]));
                map.setValue(row[3]);

                MID mid = table.insertMap(map, type);
                table.insertIndex(mid, map, type);

//                System.out.println("Inserted record " + recordNum);
            }

            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);

            // TODO ask TA : When would the buffer be freed ?
            // Reading the data inserted
            Stream stream = table.openStream(bigTableName, type, "*", "*", "*", numbuf/4);
            Map map = stream.getNext();
            while (map != null) {
//                System.out.println("---" + map.getValue());
                map = stream.getNext();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }


    }
}