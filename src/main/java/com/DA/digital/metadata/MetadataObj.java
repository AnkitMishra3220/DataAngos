package com.da.digital.metadata;

import java.io.Serializable;
import java.util.Map;

public class MetadataObj implements Serializable {

    private Map<String, ColumnAttributes> columnIndexMap;

    public void setColumnIndexMap(Map<String, ColumnAttributes> columnIndexMap){
        this.columnIndexMap=columnIndexMap;
    }

    public Map<String, ColumnAttributes> getColumnIndexMap(){return columnIndexMap;}
}

