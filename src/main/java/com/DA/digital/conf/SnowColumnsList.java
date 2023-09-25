package com.da.digital.conf;

import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SnowColumnsList implements Serializable {
    public List<String> getColList() {
        return colList;
    }

    public void setColList(List<String> colList) {
        this.colList = colList;
    }

    private List<String> colList=  new ArrayList<>();


}
