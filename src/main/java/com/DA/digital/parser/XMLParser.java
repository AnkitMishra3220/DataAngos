package com.da.digital.parser;

import com.da.digital.metadata.Constant;
import com.ximpleware.*;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class  XMLParser implements Parser<String, ParserResultContainer> {

    private static final Logger logger = LoggerFactory.getLogger(XMLParser.class);

    private boolean enableNS;

    private Map<String, List<String>> xpaths;

    public XMLParser(boolean enableNS, Map<String, List<String>> xpaths) {
        this.enableNS = enableNS;
        this.xpaths = xpaths;
    }

    @Override
    public ParserResultContainer parse(String inputXML) {

        ParserResultContainer parserResultContainer = new ParserResultContainer();

        parserResultContainer.setErrorContainer("");

        VTDGen vtdGen = new VTDGen();

        try {
            String encoding = identifyEncoding(inputXML.substring(0, 39));
            vtdGen.setDoc(inputXML.getBytes(encoding));
            vtdGen.parse(enableNS);
            VTDNav vtdNav = vtdGen.getNav();
            AutoPilot autoPilot = new AutoPilot(vtdNav);
            xpaths.forEach((k, v) -> {
                String value = getFieldValue(vtdNav, autoPilot, v);
                parserResultContainer.getSuccessContainer().put(k,value);
            });


        } catch (NullPointerException | ParseException | UnsupportedEncodingException|ArrayIndexOutOfBoundsException e) {

            logger.error("Error in XML Parsing {}" , e.getMessage());
            logger.error("Error XML {}" ,inputXML);
            parserResultContainer.setErrorContainer(e.getMessage() + "--- " +inputXML);
            xpaths.keySet().forEach(k -> parserResultContainer.getSuccessContainer().put(k, ""));
        }

        return parserResultContainer;
    }

    public String getFieldValue(VTDNav vtdNav, AutoPilot autoPilot, List<String> xpaths) {

        String value = "";

        if (!xpaths.isEmpty()) {

            for (String xpath : xpaths) {
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
                    if (!value.isEmpty()) {
                        break;
                    }

                } catch (XPathParseException | NavException | XPathEvalException ex) {
                    logger.error("Error parsing XML {} - {}" , ex.getMessage() , xpath);
                }

            }

        }

        return value;

    }

    public String identifyEncoding(String xmlStr) {

        if (xmlStr.toUpperCase().contains(StandardCharsets.UTF_16.name())) {
            return Constant.ENCODING_FORMAT_UTF16;
        } else {
            return Constant.ENCODING_FORMAT_UTF8;
        }
    }

}
