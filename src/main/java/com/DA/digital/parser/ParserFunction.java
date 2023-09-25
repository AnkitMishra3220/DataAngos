package com.da.digital.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.da.digital.metadata.ColumnAttributes;
import com.da.digital.metadata.MetadataObj;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

public class ParserFunction implements Serializable {

    private MetadataObj metadataObj;

    public ParserFunction(MetadataObj metadataObj) {
        this.metadataObj = metadataObj;
    }

    public Tuple2<Row, String>  parsingFunction(Row inputRow) {


        List<String> values = new ArrayList<>();

        ParserResultContainer parserResultContainer = new ParserResultContainer();
        XpathStringXMLParser xpathStringXMLParser = new XpathStringXMLParser();
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();

        int i = 0;

        for (Map.Entry<String, ColumnAttributes> entry : metadataObj.getColumnIndexMap().entrySet()) {
            if (entry.getValue().getParser().toLowerCase().contains("xml")) {
                XMLParser parser = new XMLParser(false, entry.getValue().getXpaths());
                if (inputRow.getString(i) == null || !inputRow.getString(i).trim().contains("<?xml")) {
                    parserResultContainer.setErrorContainer(inputRow.getString(i));
                    entry.getValue().getXpaths().keySet().forEach(x -> values.add(null));
                } else {

                    parserResultContainer = parser.parse(inputRow.getString(i));

                    values.addAll(parserResultContainer.getSuccessContainer().values());

                    if ((entry.getValue().getAdditionalNSFieldName() != null)) {

                        Map<String, String> additionalNSMap = new LinkedHashMap<>();
                        Map<String, String> successMap = new LinkedHashMap<>();
                        int k = 1;
                        boolean cond = true;
                        while (cond) {

                            String nameXpath = entry.getValue().getAdditiinalNameXpaths() + "[" + k + "]";
                            String valueXpath = entry.getValue().getAdditiinalValueXpaths() + "[" + k + "]";

                            String name = xpathStringXMLParser.parse(inputRow.getString(i), nameXpath, false);
                            String value = xpathStringXMLParser.parse(inputRow.getString(i), valueXpath, false);

                            if (name.equals("")) {
                                cond = false;
                            } else {
                                additionalNSMap.put(name, value);
                            }

                            k = k + 1;

                        }

                        String additionalNameValueJSON = gson.toJson(additionalNSMap);
                        successMap.put(entry.getValue().getAdditionalNSFieldName(), additionalNameValueJSON);
                        parserResultContainer.setSuccessContainer(successMap);
                        values.addAll(parserResultContainer.getSuccessContainer().values());
                    }

                }
            } else {
                values.add(inputRow.getString(i));
            }

            i++;
        }

        return new Tuple2<>(RowFactory.create(values.toArray()),parserResultContainer.getErrorContainer());
    }
}
