package com.da.digital.parser;

import com.da.digital.exception.DataAngosException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

import com.google.common.io.Resources;
import org.junit.Assert;

public class ParserUtilSuite {

    private ParserUtil parserUtil;

    @Before
    public void setup(){

        parserUtil = new ParserUtil();
    }

    @Test
    public void testFlattenAsMap() throws IOException, ParseException {

        URL url = Resources.getResource("test-data/test.json");
        String json = Resources.toString(url, StandardCharsets.UTF_8);
        JSONParser parser = new JSONParser();
        Object jobj = parser.parse(json);

        Assert.assertEquals(
                "{a.b=1, e=f, a.c=, g=2.3, a.d[0]=false, a.d[1]=true}",
                parserUtil.flattenJson(jobj).toString());
    }

    @Test
    public void testMappingJSON() throws DataAngosException {

        Assert.assertEquals("{mtcn=transaction_csntransactionid, " +
                        "environment=transaction_environment, " +
                        "messagetype=transaction_transactionpurpose, comment=transaction_comment}",
                parserUtil.mappingJson("src/test/resources/test-data/testMapping").toString());

    }

    @Test
    public void testReplaceNullValuesOfMap(){

        LinkedHashMap<String, String> finalMap = new LinkedHashMap<>();
        finalMap.put("mtcn","12345");
        finalMap.put("environment",null);
        finalMap.put("comment","test");
        finalMap.put("status",null);

        Assert.assertEquals("{mtcn=12345, environment=default, comment=test, status=default}",
                parserUtil.replaceNullValuesOfMap(finalMap,"default").toString());

    }

}
