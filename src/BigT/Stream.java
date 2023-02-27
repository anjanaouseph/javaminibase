/* File Stream.java */

package BigT;

import global.AttrOperator;
import global.AttrType;
import global.MapOrder;
import iterator.*;

import java.util.ArrayList;
import java.util.List;

public class Stream {

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter);

    // Methods
    public void closestream();
    public Map getNext(MID mid);
}