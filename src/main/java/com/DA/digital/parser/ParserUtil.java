package com.da.digital.parser;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.da.digital.exception.OctopusErrorCode;
import com.da.digital.exception.OctopusException;
import com.da.digital.metadata.Constant;
import com.da.digital.metadata.DataContainer;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class ParserUtil implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ParserUtil.class);

    public Map<String, String> mappingJson(String fileName) {

        Map<String, String> mapOfMetadata = new LinkedHashMap<>();
        String metadataString = "";

        try {
            metadataString = FileUtils.readFileToString(new File(fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new OctopusException(OctopusErrorCode.FILE_PARSE_EXCEPTION);
        }
        String[] lines = metadataString.split(Constant.NEW_LINE_SEPARATOR);

        for (String record : lines) {
            String[] keyValue = record.replace(Constant.SPACE_STR, "")
                    .split(Constant.METADATA_FILE_SEPARATOR);
            String key = keyValue[0].toLowerCase();
            String value = keyValue[1];
            mapOfMetadata.put(key, value);
        }

        return mapOfMetadata;

    }

    public Map<String, String> flattenJson(Object obj) {

        Map<String, Object> flattenedJsonMap = null;

        Map<String, String> flattenedJsonMapInterim = new HashMap<>();

        JSONObject jsonObject = (JSONObject) obj;

        flattenedJsonMap = JsonFlattener.flattenAsMap(jsonObject.toJSONString());

        flattenedJsonMap.forEach((k, v) -> flattenedJsonMapInterim.put(k.toLowerCase(), v != null ? v.toString() : ""));

        return flattenedJsonMapInterim;
    }

    public Map<String, String> replaceNullValuesOfMap(Map<String, String> inMap, String defaultValue) {

        LinkedHashMap<String, String> outMap = new LinkedHashMap<>();

        inMap.forEach((k, v) -> {
            if (v == null) {
                v = defaultValue;
            }
            outMap.put(k, v);
        });
        return outMap;
    }


    public Map<String, String> executeMapping(Map<String, String> flattenedJsonMap, Map<String, String> mappingJsonMap) {

        Map<String, String> finalMap = new LinkedHashMap<>();
        Map<String, String> flattenedJsonMapInterim = new LinkedHashMap<>();
        flattenedJsonMap.forEach((k, v) -> {
            String key = k.replaceAll("\\(", "").replaceAll("\\)", "");
            flattenedJsonMapInterim.put(key, v);
        });

        mappingJsonMap.forEach((k, v) -> {
            String value = flattenedJsonMapInterim.get(k.toLowerCase());
            if (value != null) {
                finalMap.put(v, value);
            }
        });

        return finalMap;

    }

    public Map<String, String> validateJson(JSONObject jsonObject, List<String> primaryKeys) {
        for (String primaryKey : primaryKeys) {
            if (!jsonObject.toJSONString().toLowerCase().contains("\""+primaryKey.toLowerCase()+"\""+":")) {
                return (createError("Primary Key is missing in Payload", jsonObject.toJSONString()));
            }

        }

        return null;
    }

    public Map<String, String> createError(String errorString, String data) {

        Map<String, String> errorMap = new LinkedHashMap<>();

        errorMap.put(Constant.ERROR, errorString);
        errorMap.put("ts", getTimestamp());
        errorMap.put("payload", data);

        return errorMap;
    }

    public String getTimestamp() {

        String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        String date = simpleDateFormat.format(new Date());
        Calendar cal = Calendar.getInstance();

        try {
            cal.setTime(simpleDateFormat.parse(date));
        } catch (java.text.ParseException e) {
            logger.error(e.getMessage());
        }
        return Long.toString(cal.getTimeInMillis());
    }

    public String createContainer(Map<String, String> outputMap, boolean enableDataContainer) {

        if(outputMap !=null && outputMap.containsKey(Constant.ERROR)){
            outputMap.put(Constant.BDP_PARSER_ERROR_SWITCH, "true");
        }

        String output = "";
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();

        if(enableDataContainer){
            output = setDataContainer(outputMap);
        }else{
            output = gson.toJson(outputMap);
        }
        return output;
    }

    public String setDataContainer(Map<String,String> outputMap){

        DataContainer dataContainer = new DataContainer();
        String mtcn = "transaction_csntransactionid";
        String event = "transaction_transactionpurpose";
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();

        if (outputMap != null && (!outputMap.containsKey(Constant.ERROR))) {
            dataContainer.setId((outputMap.get(mtcn) != null ||
                    !outputMap.get(mtcn).isEmpty()) ?
                    outputMap.get(mtcn) : "");
            dataContainer.setEvent((outputMap.get(mtcn) != null ||
                    !outputMap.get(event).isEmpty()) ?
                    outputMap.get(event) : "");
            dataContainer.setIngestionTimestamp(getTimestamp());
            dataContainer.setData(outputMap);
        }else{
            if (outputMap != null) {
                outputMap.put(Constant.BDP_PARSER_ERROR_SWITCH, "true");
                dataContainer.setId("");
                dataContainer.setEvent("");
                dataContainer.setIngestionTimestamp(getTimestamp());
                dataContainer.setData(outputMap);
            }
        }

            return gson.toJson(dataContainer);

    }
}
