package com.da.digital.parser;

import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class XMLParserSuite {

    @Test
    public void parseXMLWithXpath() throws IOException {

        String data = new String(Files.readAllBytes(Paths.get("src/test/resources/test-data/test.xml")));

        LinkedHashMap<String, List<String>> xpaths = new LinkedHashMap<>();

        xpaths.put("csntransaction_id", Collections.singletonList("/FundingSource/mtcn"));
        xpaths.put("transaction_amount", Collections.singletonList("/FundingSource/amount"));
        xpaths.put("transaction_payment_method", Collections.singletonList("/FundingSource/paymentType"));
        xpaths.put("transaction_external_processor", Collections.singletonList("/FundingSource/extProcessor"));
        xpaths.put("transaction_currency", Collections.singletonList("/FundingSource/currency"));
        xpaths.put("sender_country", Collections.singletonList("/FundingSource/billingCountry"));
        xpaths.put("transaction_country", Collections.singletonList("/FundingSource/billingCountry"));
        xpaths.put("transaction_channel_code", Collections.singletonList("/FundingSource/caller"));
        xpaths.put("transaction_payment_subtype", Collections.singletonList("/FundingSource/paymentSubType"));
        xpaths.put("transaction_state", Collections.singletonList("/FundingSource/stateMachineState"));
        xpaths.put("transaction_date", Collections.singletonList("/FundingSource/transactionDate"));

        XMLParser xmlParser = new XMLParser(false, xpaths);

        ParserResultContainer parserResultContainer = xmlParser.parse(data);

        Assert.assertEquals("0041000000127850916250",parserResultContainer.getSuccessContainer().get("csntransaction_id"));
        Assert.assertEquals("4.0375E2",parserResultContainer.getSuccessContainer().get("transaction_amount"));
        Assert.assertEquals("CreditCard",parserResultContainer.getSuccessContainer().get("transaction_payment_method"));
        Assert.assertEquals("PAYMENTECH",parserResultContainer.getSuccessContainer().get("transaction_external_processor"));
        Assert.assertEquals("USD",parserResultContainer.getSuccessContainer().get("transaction_currency"));
        Assert.assertEquals("US",parserResultContainer.getSuccessContainer().get("sender_country"));
        Assert.assertEquals("US",parserResultContainer.getSuccessContainer().get("transaction_country"));
        Assert.assertEquals("SPEEDPAY",parserResultContainer.getSuccessContainer().get("transaction_channel_code"));
        Assert.assertEquals("VISA",parserResultContainer.getSuccessContainer().get("transaction_payment_subtype"));
        Assert.assertEquals("Settled",parserResultContainer.getSuccessContainer().get("transaction_state"));
        Assert.assertEquals("2018-12-28T13:16:25.763-05:00",parserResultContainer.getSuccessContainer().get("transaction_date"));
    }

}
