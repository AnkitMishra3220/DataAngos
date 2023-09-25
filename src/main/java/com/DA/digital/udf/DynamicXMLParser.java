package com.da.digital.udf;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class DynamicXMLParser implements UDF, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DynamicXMLParser.class);

    @Autowired
    private SparkSession spark;

    @Override
    public void callUDF() {

        spark.udf().register("dynamicXMLParser", (String xml) -> {


            Map<String, String> pasingResults = new LinkedHashMap<>();
            String key = "";

            XMLInputFactory factory = XMLInputFactory.newInstance();

            try {

                XMLEventReader eventReader =
                        factory.createXMLEventReader(new StringReader(xml));

                while (eventReader.hasNext()) {

                    XMLEvent event = eventReader.nextEvent();


                    if (event.isStartElement()) {
                        StartElement element = (StartElement) event;

                        String qNameXML = element.getName().toString();

                        if (qNameXML.contains("{")) {

                            String nameSpace = StringEscapeUtils
                                    .unescapeXml(StringUtils.substringBetween(qNameXML, "{", "}"));

                            String replaceNS = qNameXML.replaceAll(nameSpace, "")
                                    .replaceAll("\\{", "")
                                    .replaceAll("\\}", "");
                            key = replaceNS;

                        } else {
                            key = qNameXML;
                        }

                        Iterator<Attribute> iterator = element.getAttributes();

                        while (iterator.hasNext()) {
                            Attribute attribute = iterator.next();
                            QName name = attribute.getName();
                            String value = attribute.getValue();
                            pasingResults.put(name.toString(), value);

                        }

                    }

                    if (event.isCharacters()) {
                        Characters element = (Characters) event;

                        if (!(element.getData().trim().isEmpty())) {
                            pasingResults.put(key, element.getData());
                        }
                    }
                }
            } catch (NullPointerException | javax.xml.stream.XMLStreamException e) {

                logger.error("Error in Parsing Dynamic XML: {} {} {}" , e.getMessage() , "Error XML Record" , xml);
            }

            return pasingResults;


        }, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    }
}
