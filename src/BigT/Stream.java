package BigT;

import global.AttrOperator;
import global.AttrType;
import global.MapOrder;
import iterator.*;

import java.util.ArrayList;
import java.util.List;

public class Stream {

    int indexType , orderType ,numBuf;
    MapIterator map_iterator;
    SortMap sortMap;

    CondExpr[] con_expers;
    CondExpr[] indexfilter_2;
    CondExpr[] indexfilter_3;
    CondExpr[] indexfilter_4;
    CondExpr[] indexfilter_5;

    public Stream(String bigtName, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        //this.indexType = indexType;
        this.orderType = orderType;
        this.numBuf = numBuf;
        List<CondExpr> expres = new ArrayList<CondExpr>();
        expres.addAll(filtertype_process(rowFilter, 1));
        expres.addAll(filtertype_process(columnFilter, 2));
        expres.addAll(filtertype_process(valueFilter, 4));

        con_expers = new CondExpr[expres.size() + 1];
        int i = 0;
        for (CondExpr expr : expres) {
            con_expers[i++] = expr;
        }
        con_expers[i] = null;

        indexfilter_2 = keyfilter_indextype(2, rowFilter, columnFilter, valueFilter);
        indexfilter_3 = keyfilter_indextype(3, rowFilter, columnFilter, valueFilter);
        indexfilter_4 = keyfilter_indextype(4, rowFilter, columnFilter, valueFilter);
        indexfilter_5 = keyfilter_indextype(5, rowFilter, columnFilter, valueFilter);


        try {
            switch (indexType) {
                case 1:
                    map_iterator = new FileScanMap(bigtName, null, con_expers, true);
                    break;
                default:
                    map_iterator = new CombinedScanMap(bigtName, con_expers, indexfilter_2, indexfilter_3, indexfilter_4, indexfilter_5);
                    break;
            }
            sortMap = new SortMap(null, null, null, map_iterator, this.orderType, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while initiating the stream");
        }
    }


    public void closestream() {
        try {
            sortMap.close();
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("Exception occurred while closing the stream!");
        }
    }



    public Map getNext() {
        try {
            return sortMap.get_next();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while iterating through stream!");
            return null;
        }
    }
    public static CondExpr[] keyfilter_indextype(int indexType, String rowFilter, String columnFilter, String valueFilter) {
        List<CondExpr> key_expres = new ArrayList<CondExpr>();
        switch(indexType) {
            case 1:
                key_expres.addAll(filtertype_process(valueFilter, 4));
                break;
            case 2:
                key_expres.addAll(filtertype_process(rowFilter, 1));
                break;
            case 3:
                key_expres.addAll(filtertype_process(columnFilter, 2));
                break;
            case 4:
                List<CondExpr> key_columnFilter = filtertype_process(columnFilter, 2);
                if(key_columnFilter.isEmpty()) {
                    break;
                }
                List<CondExpr> key_rowFilter = filtertype_process(rowFilter, 1);
                if(key_rowFilter.isEmpty()) {
                    key_expres.addAll(key_columnFilter);
                    break;
                }
                int size_columnFilter = key_columnFilter.size();
                int size_rowFilter = key_rowFilter.size();
                if(size_columnFilter == 1 && size_rowFilter == 1) {
                    CondExpr expres = new CondExpr();
                    expres.op = new AttrOperator(AttrOperator.aopEQ);
                    expres.type1 = new AttrType(AttrType.attrSymbol);
                    expres.type2 = new AttrType(AttrType.attrString);
                    expres.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres.operand2.string = key_columnFilter.get(0).operand2.string + "%" + key_rowFilter.get(0).operand2.string;
                    expres.next = null;
                    key_expres.add(expres);
                    break;
                } else if(size_columnFilter == 1 && size_rowFilter == 2) {
                    CondExpr expres1 = new CondExpr();
                    expres1.op = new AttrOperator(AttrOperator.aopGE);
                    expres1.type1 = new AttrType(AttrType.attrSymbol);
                    expres1.type2 = new AttrType(AttrType.attrString);
                    expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres1.operand2.string = key_columnFilter.get(0).operand2.string + "%" + key_rowFilter.get(0).operand2.string;;
                    expres1.next = null;
                    CondExpr expres2 = new CondExpr();
                    expres2.op = new AttrOperator(AttrOperator.aopLE);
                    expres2.type1 = new AttrType(AttrType.attrSymbol);
                    expres2.type2 = new AttrType(AttrType.attrString);
                    expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres2.operand2.string = key_columnFilter.get(0).operand2.string + "%" + key_rowFilter.get(1).operand2.string;;
                    expres2.next = null;
                    key_expres.add(expres1);
                    key_expres.add(expres2);
                } else if(size_columnFilter == 2 && size_rowFilter == 1) {
                    CondExpr expres1 = new CondExpr();
                    expres1.op = new AttrOperator(AttrOperator.aopGE);
                    expres1.type1 = new AttrType(AttrType.attrSymbol);
                    expres1.type2 = new AttrType(AttrType.attrString);
                    expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres1.operand2.string = key_columnFilter.get(0).operand2.string + "%" + key_rowFilter.get(0).operand2.string;;
                    expres1.next = null;
                    CondExpr expres2 = new CondExpr();
                    expres2.op = new AttrOperator(AttrOperator.aopLE);
                    expres2.type1 = new AttrType(AttrType.attrSymbol);
                    expres2.type2 = new AttrType(AttrType.attrString);
                    expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres2.operand2.string = key_columnFilter.get(1).operand2.string + "%" + key_rowFilter.get(0).operand2.string;;
                    expres2.next = null;
                    key_expres.add(expres1);
                    key_expres.add(expres2);
                } else if(size_columnFilter == 2 && size_rowFilter == 2) {
                    CondExpr expres1 = new CondExpr();
                    expres1.op = new AttrOperator(AttrOperator.aopGE);
                    expres1.type1 = new AttrType(AttrType.attrSymbol);
                    expres1.type2 = new AttrType(AttrType.attrString);
                    expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres1.operand2.string = key_columnFilter.get(0).operand2.string + "%" + key_rowFilter.get(0).operand2.string;;
                    expres1.next = null;
                    CondExpr expres2 = new CondExpr();
                    expres2.op = new AttrOperator(AttrOperator.aopLE);
                    expres2.type1 = new AttrType(AttrType.attrSymbol);
                    expres2.type2 = new AttrType(AttrType.attrString);
                    expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expres2.operand2.string = key_columnFilter.get(1).operand2.string + "%" + key_rowFilter.get(1).operand2.string;;
                    expres2.next = null;
                    key_expres.add(expres1);
                    key_expres.add(expres2);
                }
                break;
            case 5:
                List<CondExpr> key_rowFilter_2 = filtertype_process(rowFilter, 1);
                if(key_rowFilter_2.isEmpty()) {
                    break;
                }
                List<CondExpr> valueKeyFilter = filtertype_process(valueFilter, 4);
                if(valueKeyFilter.isEmpty()) {
                    key_expres.addAll(key_rowFilter_2);
                    break;
                }
                int key_rowFilter_2Size = key_rowFilter_2.size();
                int valueKeyFilterSize = valueKeyFilter.size();
                if(key_rowFilter_2Size == 1 && valueKeyFilterSize == 1) {
                    CondExpr expres = new CondExpr();
                    expres.op = new AttrOperator(AttrOperator.aopEQ);
                    expres.type1 = new AttrType(AttrType.attrSymbol);
                    expres.type2 = new AttrType(AttrType.attrString);
                    expres.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres.operand2.string = key_rowFilter_2.get(0).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(0).operand2.string);
                    expres.next = null;
                    key_expres.add(expres);
                    break;
                } else if(key_rowFilter_2Size == 1 && valueKeyFilterSize == 2) {
                    CondExpr expres1 = new CondExpr();
                    expres1.op = new AttrOperator(AttrOperator.aopGE);
                    expres1.type1 = new AttrType(AttrType.attrSymbol);
                    expres1.type2 = new AttrType(AttrType.attrString);
                    expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres1.operand2.string = key_rowFilter_2.get(0).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(0).operand2.string);
                    expres1.next = null;
                    CondExpr expres2 = new CondExpr();
                    expres2.op = new AttrOperator(AttrOperator.aopLE);
                    expres2.type1 = new AttrType(AttrType.attrSymbol);
                    expres2.type2 = new AttrType(AttrType.attrString);
                    expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres2.operand2.string = key_rowFilter_2.get(0).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(1).operand2.string);
                    expres2.next = null;
                    key_expres.add(expres1);
                    key_expres.add(expres2);
                } else if(key_rowFilter_2Size == 2 && valueKeyFilterSize == 1) {
                    CondExpr expres1 = new CondExpr();
                    expres1.op = new AttrOperator(AttrOperator.aopGE);
                    expres1.type1 = new AttrType(AttrType.attrSymbol);
                    expres1.type2 = new AttrType(AttrType.attrString);
                    expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres1.operand2.string = key_rowFilter_2.get(0).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(0).operand2.string);
                    expres1.next = null;
                    CondExpr expres2 = new CondExpr();
                    expres2.op = new AttrOperator(AttrOperator.aopLE);
                    expres2.type1 = new AttrType(AttrType.attrSymbol);
                    expres2.type2 = new AttrType(AttrType.attrString);
                    expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres2.operand2.string = key_rowFilter_2.get(1).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(0).operand2.string);
                    expres2.next = null;
                    key_expres.add(expres1);
                    key_expres.add(expres2);
                } else if(key_rowFilter_2Size == 2 && valueKeyFilterSize == 2) {
                    CondExpr expres1 = new CondExpr();
                    expres1.op = new AttrOperator(AttrOperator.aopGE);
                    expres1.type1 = new AttrType(AttrType.attrSymbol);
                    expres1.type2 = new AttrType(AttrType.attrString);
                    expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres1.operand2.string = key_rowFilter_2.get(0).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(0).operand2.string);
                    expres1.next = null;
                    CondExpr expres2 = new CondExpr();
                    expres2.op = new AttrOperator(AttrOperator.aopLE);
                    expres2.type1 = new AttrType(AttrType.attrSymbol);
                    expres2.type2 = new AttrType(AttrType.attrString);
                    expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expres2.operand2.string = key_rowFilter_2.get(1).operand2.string + "%" + getvaluefilter(valueKeyFilter.get(1).operand2.string);
                    expres2.next = null;
                    key_expres.add(expres1);
                    key_expres.add(expres2);
                }
                break;
        }
        CondExpr[] res = new CondExpr[key_expres.size() + 1];
        int i = 0;
        for (CondExpr expres : key_expres) {
            res[i++] = expres;
        }
        res[i] = null;
        return res;
    }

    public static String getvaluefilter(String valueFilterString){
        int len = valueFilterString.length();
        for(int i = len; i < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; i++){
            valueFilterString = "0"+valueFilterString;
        }
        return valueFilterString;
    }

    private static List<CondExpr> filtertype_process(String filter, int fldNum) {
        List<CondExpr> res = new ArrayList<CondExpr>();
        if (filter.equals("*")) {

        } else if (filter.contains("[")) {
            String[] filter_split = filter.substring(1, filter.length() - 1).split(",");
            CondExpr expres1 = new CondExpr();
            expres1.op = new AttrOperator(AttrOperator.aopGE);
            expres1.type1 = new AttrType(AttrType.attrSymbol);
            expres1.type2 = new AttrType(AttrType.attrString);
            expres1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            if(fldNum == 4){
                expres1.operand2.string = getvaluefilter(filter_split[0]);
            }else{
                expres1.operand2.string = filter_split[0];
            }
            expres1.next = null;
            CondExpr expres2 = new CondExpr();
            expres2.op = new AttrOperator(AttrOperator.aopLE);
            expres2.type1 = new AttrType(AttrType.attrSymbol);
            expres2.type2 = new AttrType(AttrType.attrString);
            expres2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            if(fldNum == 4){
                expres2.operand2.string = getvaluefilter(filter_split[1]);
            }else{
                expres2.operand2.string = filter_split[1];
            }
            expres2.next = null;
            res.add(expres1);
            res.add(expres2);
        } else {
            CondExpr expres = new CondExpr();
            expres.op = new AttrOperator(AttrOperator.aopEQ);
            expres.type1 = new AttrType(AttrType.attrSymbol);
            expres.type2 = new AttrType(AttrType.attrString);
            expres.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            if(fldNum == 4){
                expres.operand2.string = getvaluefilter(filter);
            }else{
                expres.operand2.string = filter;
            }
            expres.next = null;
            res.add(expres);
        }
        return res;
    }
}





