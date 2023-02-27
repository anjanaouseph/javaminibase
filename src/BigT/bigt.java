/* File bigt.java */

package BigT;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import bufmgr.*;
import global.*;
import btree.*;
import heap.*;
import iterator.*;
import index.*;
import java.util.*;

public class bigt {
    private String name;

    private Heapfile _hf;
    public ArrayList<Heapfile> heapFiles;
    public ArrayList<String> heapFileNames;
    public ArrayList<String> indexFileNames;
    public ArrayList<BTreeFile> indexFiles;
    public BTreeFile utilityIndex = null;
    public String indexUtil;
    private AttrType[] attrType;
    private FldSpec[] projlist;
    private CondExpr[] expr;
    MapIndexScan iscan;
    private int insertType;
    short[] res_str_sizes = new short[]{Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};

    public bigt(String name, boolean insert) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException, btree.IteratorException,
            btree.UnpinPageException, btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException,
            PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        String fileName = "";
        heapFiles = new ArrayList<>(6);
        indexFiles = new ArrayList<>(6);
        heapFileNames = new ArrayList<>(6);
        indexFileNames = new ArrayList<>(6);
        this.name = name;
        heapFiles.add(null);
        heapFileNames.add("");
        indexFileNames.add("");
        for(int i = 1; i <= 5; i++){
            heapFileNames.add(name + "_" + i);
            indexFileNames.add(name + "_index_" + i);
            heapFiles.add(new Heapfile(heapFileNames.get(i)));
        }

        indexUtil = name + "_" + "indexUtil";

        if(insert){
            //For multiple batch insert

            indexCreateUtil();
            createIndexUtil();

            try {
                deleteAllNodesInIndex(utilityIndex);
                for(int i=2; i<=5; i++) {
                    deleteAllNodesInIndex(indexFiles.get(i));
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Exception occurred while destroying all nodes of index files");
            }
        }
        initExprs();
    }

    public void initExprs(){

    }
    public String getName(){
        return name;
    }

    public Heapfile getHeapFile(int i) {
        return heapFiles.get(i);
    }

    public int getType(){
        return this.storageType;
    }

    public String getIndexFileName(int i){
        return indexFileNames.get(i);
    }

    public String getHeapFileName(int i){
        return heapFileNames.get(i);
    }

    public String indexName(){
        return name + "_index_" + storageType;
    }


    }