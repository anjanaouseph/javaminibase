package BigT;

import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class BatchInsert {

    /**
     * Constructor
     * @param datafile Path to the data file
     * @param type Index type
     * @param bigTableName Name of the table to be created or retrieved
     * @param numbuf Number of buffers to be used.
     */
    public BatchInsert(String datafile, int type,  String bigTableName, int numbuf) {
        System.out.println("Starting to read from the data file : " + datafile);
        System.out.println("Index type : " + type);
        System.out.println("Table name : " + bigTableName + "_" + type);
        System.out.println("Number of buffers : " + numbuf);


        try {
            // Check if the data file exists
            try {
                Paths.get(datafile);
            } catch (InvalidPathException exception) {
                System.out.println("Exception in getting the file with the provided path. Check the path and try again !");
                throw new Exception(exception);
            }

            // Checking if the DB is already created.
            if (SystemDefs.JavabaseDB == null) {
                // Initialize the data base.
                String dbpath = "/tmp/batch-insert"+System.getProperty("user.name")+".minibase-db";
                SystemDefs sysdef = new SystemDefs( dbpath, 1000000, numbuf, "Clock" );
            }

            // Calling the constructor with the data.
            // Since we retrieve/create the heap files with a standard name. If the table was already created, the right file would be fetched.
            bigt table = new bigt(bigTableName, type);

            List<String> lines = Files.readAllLines(Paths.get(datafile), StandardCharsets.UTF_8);
            if (!lines.isEmpty() && lines.get(0).startsWith("\uFEFF")) {
                // Remove the UTF-8 BOM character from the first line
                lines.set(0, lines.get(0).substring(1));
            }
            List<String[]> rows = lines.stream().map(line -> line.split(",")).collect(Collectors.toList());;

            int recordNum = 0;
            for (String[] row : rows) {
                recordNum++;
                // reading each row
                if (row.length != 4) {
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
            }

            System.out.println("INSERTED RECORDS : " + recordNum);
            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
