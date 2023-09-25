package com.da.digital.parser;

import com.da.digital.metadata.Constant;
import com.ximpleware.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class XpathStringXMLParser implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(XpathStringXMLParser.class);

    public String parse(String xml,String xPath,boolean enableNS)  {

        String pasingResults = "";
        VTDGen vtdGen = new VTDGen();

        try {
            vtdGen.setDoc(xml.getBytes(StandardCharsets.UTF_8.name().toLowerCase()));
            vtdGen.parse(enableNS);
            VTDNav vtdNav = vtdGen.getNav();
            AutoPilot autoPilot = new AutoPilot(vtdNav);
            pasingResults = getFieldValue(vtdNav, autoPilot, xPath);

        } catch (NullPointerException | ParseException | UnsupportedEncodingException | ArrayIndexOutOfBoundsException e) {

            logger.error("Error in XML Parsing {}" , e.getMessage());
            logger.error("Error XML {}" , xml);
        }

        return pasingResults;
    }

    public String getFieldValue(VTDNav vtdNav, AutoPilot autoPilot, String xpath) {

        String value = "";

        if (!xpath.isEmpty()) {

            try {
                autoPilot.selectXPath(xpath);

                if (xpath.contains(Constant.AT_SIGN)) {
                    value = autoPilot.evalXPathToString();
                } else {
                    while (autoPilot.evalXPath() != -1) {

                        if (vtdNav.getText() != -1) {
                            value = vtdNav.toNormalizedString(vtdNav.getText());
                        }
                    }
                }

                autoPilot.resetXPath();

            } catch (XPathParseException | NavException | XPathEvalException ex) {
                logger.error("Error parsing XML {} - {}" , ex.getMessage() ,xpath);
            }


        }

        return value;

    }
}
