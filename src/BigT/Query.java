package BigT;

import diskmgr.PCounter;

public class Query {
    public Query(String bigtName, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {

        try {
            // Calling the constructor with the bigtable name and type
            bigt table = new bigt(bigtName);

            // Retrieving the data inserted based on the query
            Stream stream = table.openStream(bigtName, orderType, rowFilter, columnFilter, valueFilter, numBuf);

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
