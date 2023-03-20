/* File Map.java */

package BigT;

import java.io.IOException;
import java.util.Arrays;

import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.InvalidTupleSizeException;
import heap.*;


public class Map implements GlobalConst {

    /* Byte array to store data */

    private byte[] data;

    /* Map will have 4 fixed fields */
    private static short fldCnt = 4;

    /* Start position of this map in data[] */
    private int map_offset;

    private int map_length;

    /* Array of offsets of the fields */

    private short[] fldOffset;

    public static final int DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE = 20;

    public static final int DEFAULT_STRING_ATTRIBUTE_SIZE = 20;
    public static final int noOfStrFields = 3;

    public static final int MAX_MAP_LENGTH = MINIBASE_PAGESIZE;

    public Map() throws IOException {
        this.data = new byte[MAX_MAP_LENGTH];
        this.map_offset = 0;
        this.map_length = MAX_MAP_LENGTH;
    }

    public Map(Map fromMap) {
        this.data = fromMap.getMapByteArray();
        this.map_offset = 0;
        this.map_length = fromMap.getLength();
        fldCnt = fromMap.noOfFlds();
        fldOffset = fromMap.copyFldOffset();
    }

    public Map(int size){
        data = new byte[size];
        map_offset = 0;
        map_length = size;
    }

    public Map(byte[] amap, int offset) {
        this.data = amap;
        this.map_offset = offset;
    }
    public Map(byte[] amap, int offset, int len) {
        this.data = amap;
        this.map_offset = offset;
        this.map_length = len;
    }

    public void setHdr(short numFlds, AttrType types[], short strSizes[]) throws IOException, InvalidTupleSizeException {
        if ((numFlds + 2) * 2 > MAX_MAP_LENGTH)
            throw new InvalidTupleSizeException(null, "MAP: MAP_TOOBIG_ERROR");
        fldCnt = numFlds;
        Convert.setShortValue(numFlds, map_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = map_offset + 2; // Used first 2 bytes from map_offset tp set numFlds short value in data

        fldOffset[0] = (short) ((numFlds + 2) * 2 + map_offset);
        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2; //Another 2 bytes used to store the fldOffset[0] which is basically denoting the start of actual data

        // We know that the attribute type orders are String, String, Integer and String.
        short strCount = 0;
        short incr = 0;
        for(int i = 1; i<=numFlds; i++){
            if(i == 3){
                incr = 4;
            }else{
                incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                strCount++;
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;
        }
        map_length = fldOffset[numFlds] - map_offset;
        if(map_length > MAX_MAP_LENGTH){
            throw new InvalidTupleSizeException(null, "Map: MAP_TOOBIG_ERROR_AFTER_ALLOC");
        }
    }

    public void setDefaultHdr() throws IOException, InvalidTupleSizeException {
        short numFlds = 4;
        if ((numFlds + 2) * 2 > MAX_MAP_LENGTH)
            throw new InvalidTupleSizeException(null, "MAP: MAP_TOOBIG_ERROR");
        fldCnt = numFlds;
        Convert.setShortValue(numFlds, map_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = map_offset + 2; // Used first 2 bytes from map_offset tp set numFlds short value in data

        fldOffset[0] = (short) ((numFlds + 2) * 2 + map_offset);
        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2; //Another 2 bytes used to store the fldOffset[0] which is basically denoting the start of actual data

        // We know that the attribute type orders are String, String, Integer and String.
        short incr = 0;
        for(int i = 1; i<=numFlds; i++){
            if(i == 3){
                incr = 4;
            }else if(i == 1){
                incr = (short) (DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + 2);
            }else{
                incr = (short) (DEFAULT_STRING_ATTRIBUTE_SIZE + 2);  //strlen in bytes = strlen +2
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;
        }
        map_length = fldOffset[numFlds] - map_offset;
        if(map_length > MAX_MAP_LENGTH){
            throw new InvalidTupleSizeException(null, "Map: MAP_TOOBIG_ERROR_AFTER_ALLOC");
        }
    }

    public String getRowLabel() throws IOException {
        String rowLabel;
        rowLabel = Convert.getStrValue(fldOffset[0], this.data, fldOffset[1] - fldOffset[0]);
        return rowLabel;
    }

    public Map setRowLabel(String rowLabel) throws IOException {
        Convert.setStrValue(rowLabel, fldOffset[0], this.data);
        return this;
    }

    public String getColumnLabel() throws IOException {
        String columnLabel;
        columnLabel = Convert.getStrValue(fldOffset[1], this.data, fldOffset[2] - fldOffset[1]);
        return columnLabel;
    }

    public Map setColumnLabel(String columnLabel) throws IOException {
        Convert.setStrValue(columnLabel, fldOffset[1], this.data);
        return this;
    }

    public int getTimeStamp() throws IOException {
        int val;
        val = Convert.getIntValue(fldOffset[2], this.data);

        return val;
    }

    public Map setTimeStamp(int timeStamp) throws IOException {
        Convert.setIntValue(timeStamp, fldOffset[2], this.data);
        return this;
    }

    public String getValue() throws IOException {
        String value;
        value = Convert.getStrValue(fldOffset[3], this.data, fldOffset[4] - fldOffset[3]);
        return value;
    }

    public Map setValue(String value) throws IOException {
        Convert.setStrValue(value, fldOffset[3], this.data);
        return this;
    }

    public byte[] getMapByteArray() {
        byte[] mapCopy = new byte[map_length];
        System.arraycopy(data, map_offset, mapCopy, 0, map_length);
        return mapCopy;
    }

    public void print() throws IOException {
        String rowLabel = getRowLabel();
        String columnLabel = getColumnLabel();
        int timeStamp = getTimeStamp();
        String value = getValue();
        Long convertedVal = Long.valueOf(value);
        System.out.println("[" + rowLabel + " " + columnLabel + " " + timeStamp + " ] -> " + convertedVal);
    }

    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    public void mapCopy(Map fromMap) {
        byte[] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, data, map_offset, map_length);
    }

    public byte[] returnMapByteArray() {
        return data;
    }

    public void mapInit(byte[] amap, int offset) {
        this.data = amap;
        this.map_offset = offset;
    }

    public void mapInit(byte[] amap, int offset, int len) {
        this.data = amap;
        this.map_offset = offset;
        this.map_length = len;
    }

    public void mapSet(byte[] frommap, int offset, int len) {
        System.arraycopy(frommap, offset, this.data, 0, len);
        this.map_offset = 0;
        this.map_length = len;
    }

    public int getOffset() {
        return map_offset;
    }
    public int getLength() {
        return map_length;
    }
    public short noOfFlds() {
        return fldCnt;
    }

    public short[] copyFldOffset() {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i = 0; i <= fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

    public void setFldOffset(byte[] mapByteArray)throws IOException{
        byte[] fld = Arrays.copyOfRange(mapByteArray, 2, (fldCnt+2)*2);
        int size = fld.length;
        fldOffset = new short[fldCnt+1];
        int pos = 0;
        for(int i = 0; i < size; i+=2){
            fldOffset[pos] = Convert.getShortValue(i, fld);
            pos+=1;
        }
    }

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setIntValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setStrValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setFloValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");

    }

}