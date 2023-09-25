package com.da.digital.metadata;

import lombok.Data;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

@Data
public class ColumnAttributes implements Serializable {

    private String name;
    private String parser;
    private LinkedHashMap<String, List<String>> xpaths;
    private String additionalNSFieldName;
    private String additiinalNameXpaths;
    private String additiinalValueXpaths;
}
