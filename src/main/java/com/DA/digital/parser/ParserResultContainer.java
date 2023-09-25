package com.da.digital.parser;

import java.util.LinkedHashMap;
import java.util.Map;

public class ParserResultContainer {

    private Map<String, String> successContainer = new LinkedHashMap<>();

    private String errorContainer;


    public void setSuccessContainer(Map<String, String> successContainer) {
        this.successContainer = successContainer;
    }

    public Map<String, String> getSuccessContainer() {
        return successContainer;
    }

    public void setErrorContainer(String errorContainer){
        this.errorContainer = errorContainer;
    }

    public String getErrorContainer(){
        return errorContainer;
    }
}