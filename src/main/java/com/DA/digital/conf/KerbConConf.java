package com.da.digital.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class KerbConConf {

    Logger logger = LoggerFactory.getLogger(KerbConConf.class);

    @Autowired
    private AppConfig appConfig;

    public void setKerbConConfig() {

        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        try {
            UserGroupInformation.loginUserFromKeytab(appConfig.getKeytabUser(), appConfig.getKeytabLocation());
        } catch (IOException ex) {
            logger.info(ex.getMessage() , "KeyTab File is not exist");

        }

    }


}
