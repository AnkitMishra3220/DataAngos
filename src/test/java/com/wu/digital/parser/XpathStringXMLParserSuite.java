package com.da.digital.parser;

import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

public class XpathStringXMLParserSuite {

    private static final Logger logger = LoggerFactory.getLogger(XpathStringXMLParserSuite.class);

    @Test
    public void parseXMLWithXpath() throws DataAngosException {

        String data = "";

        try{
            data = new String(Files.readAllBytes(Paths.get("src/test/resources/test-data/test.xml")));
        }catch (Exception ex){
            logger.error(ex.getMessage());
            throw new DataAngosException(DataAngosErrorCode.IO_FILE_ERROR);
        }

        XpathStringXMLParser xpathStringXMLParser = new XpathStringXMLParser();

        Assert.assertEquals("0041000000127850916250",xpathStringXMLParser.parse(data,"/FundingSource/mtcn",false));
        Assert.assertEquals("4.0375E2",xpathStringXMLParser.parse(data,"/FundingSource/amount",false));
        Assert.assertEquals("CreditCard",xpathStringXMLParser.parse(data,"/FundingSource/paymentType",false));
        Assert.assertEquals("PAYMENTECH",xpathStringXMLParser.parse(data,"/FundingSource/extProcessor",false));
        Assert.assertEquals("USD",xpathStringXMLParser.parse(data,"/FundingSource/currency",false));
    }
}
