package BigT;

import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;
import heap.Heapfile;
import index.MapIndexScan;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
            // Checking if the DB is already created.
//            if (SystemDefs.JavabaseDB == null) {
//                // Initialize the data base.
//                String dbpath = "/tmp/"+ bigTableName + "_" + type  + ".minibase-db";
//                try {
//                    // DB exists.
//                    Paths.get(dbpath);
//                    SystemDefs.MINIBASE_RESTART_FLAG = true;
//                    SystemDefs sysdef = new SystemDefs( dbpath, 1000000, numbuf, "Clock" );
//                } catch (InvalidPathException exception) {
//                    // The db does not exist.
//                    SystemDefs sysdef = new SystemDefs( dbpath, 1000000, numbuf, "Clock" );
//                }
//            }

            if (SystemDefs.JavabaseDB == null) {
                // Initialize the data base.
                String dbpath = "/tmp/batch-insert"+System.getProperty("user.name")+".minibase-db";
                SystemDefs sysdef = new SystemDefs( dbpath, 1000000, numbuf, "Clock" );
            }

            // Check if the data file exists
            try {
                Paths.get(datafile);
            } catch (InvalidPathException exception) {
                System.out.println("Exception in getting the file with the provided path. Check the path and try again !");
                throw new Exception(exception);
            }


            // Calling the constructor with the data.
            // Since we retrieve/create the heap files with a standard name. If the table was already created, the right file would be fetched.
            bigt table = new bigt(bigTableName, type);

            List<String> lines = Files.readAllLines(Paths.get(datafile), StandardCharsets.UTF_8);
            if (!lines.isEmpty() && lines.get(0).startsWith("\uFEFF")) {
                // Remove the UTF-8 BOM character from the first line
                lines.set(0, lines.get(0).substring(1));
            }
            // There is no header
            List<String[]>  rows = lines.stream().map(line -> line.split(",")).collect(Collectors.toList());

            int recordNum = 0;
            int insertTypeFileIndex = type == 1 ? 5 : type-1;
            Heapfile tempFile = table.getHeapFile(insertTypeFileIndex);
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

               table.insertMap(map, insertTypeFileIndex);
            }

            Stream stream = new Stream(bigTableName, 1, "*", "*", "*", numbuf/2);
            Map reading = stream.getNext();

            String oldRowLabel = null;
            String oldColumnLabel = null;

            if (reading != null) {
                oldRowLabel = reading.getRowLabel();
                oldColumnLabel = reading.getColumnLabel();
            }

            List<Map> tempMaps = new ArrayList<>();

            int noDuplicateRecordCount = 0;

            while(reading != null) {

                if (!reading.getRowLabel().equals(oldRowLabel) || !reading.getColumnLabel().equals(oldColumnLabel)) {
                    oldRowLabel = reading.getRowLabel();
                    oldColumnLabel = reading.getColumnLabel();

                    if (tempMaps.size() > 3) {
                        // This means we have to remove few maps as we only have to maintain at most 3 maps.

                        // Sorting the maps in descending order
                        Collections.sort(tempMaps, (a, b) -> {
                            try {
                                return b.getTimeStamp() - a.getTimeStamp();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });

                        // Insert the first 3
                        int i = 0;
                        while(i < 3) {
                            // insert the rows into the table
                            Map map = new Map();
                            map.setDefaultHdr();
                            map.setRowLabel(tempMaps.get(i).getRowLabel());
                            map.setColumnLabel(tempMaps.get(i).getColumnLabel());
                            map.setTimeStamp(tempMaps.get(i).getTimeStamp());
                            map.setValue(tempMaps.get(i).getValue());

                            MID mid = table.insertMap(map, type);
                            table.insertIndex(mid, map, type);
                            map.print();

                            i++;
                            noDuplicateRecordCount++;
                        }
                    } else {
                        // Just insert all the records
                        int i = 0;
                        while(i < tempMaps.size()) {
                            // insert the rows into the table
                            Map map = new Map();
                            map.setDefaultHdr();
                            map.setRowLabel(tempMaps.get(i).getRowLabel());
                            map.setColumnLabel(tempMaps.get(i).getColumnLabel());
                            map.setTimeStamp(tempMaps.get(i).getTimeStamp());
                            map.setValue(tempMaps.get(i).getValue());

                            MID mid = table.insertMap(map, type);
                            table.insertIndex(mid, map, type);

//                            MID mid = tempFile.insertRecordMap(map.getMapByteArray());
//                            table.insertIndex(mid, map, type);

                            map.print();
                            i++;
                            noDuplicateRecordCount++;
                        }
                    }

                    // clear tempMap
                    tempMaps.clear();
                }


                // Add it to the list
                Map map = new Map();
                map.setDefaultHdr();
                map.setRowLabel(reading.getRowLabel());
                map.setColumnLabel(reading.getColumnLabel());
                map.setTimeStamp(reading.getTimeStamp());
                map.setValue(reading.getValue());

                tempMaps.add(map);

                reading = stream.getNext();
            }

            table.getHeapFile(insertTypeFileIndex).deleteFileMap();
            table.heapFiles.set(insertTypeFileIndex, new Heapfile(table.heapFileNames.get(insertTypeFileIndex)));
            stream.closestream();

            // Stats
            System.out.println("INSERTED RECORDS : " + recordNum);
            System.out.println("INSERTED NON DUPLICATE RECORDS : " + noDuplicateRecordCount);
            System.out.println("READ COUNT : " + PCounter.rCounter);
            System.out.println("WRITE COUNT : " + PCounter.wCounter);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
