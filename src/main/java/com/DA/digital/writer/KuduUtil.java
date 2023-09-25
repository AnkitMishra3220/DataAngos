package com.da.digital.writer;

import com.da.digital.conf.KuduConfig;
import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import org.apache.kudu.client.CreateTableOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class KuduUtil implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(KuduUtil.class);


    public static boolean existKuduTable(KuduConfig kuduConfig,String kuduTableName) {
        return kuduConfig.kuduContext().tableExists(kuduTableName);
    }


    public static boolean createKuduTable(KuduConfig kuduConfig, String kuduTableName,StructType schema) throws DataAngosException {


        if (!KuduUtil.existKuduTable(kuduConfig,kuduTableName)) {

            try {
                CreateTableOptions createTableOptions = new CreateTableOptions();

                createTableOptions.addHashPartitions(kuduConfig.getPartitionColumn(), Integer.
                        parseInt(kuduConfig.getPartitionNum()));
                createTableOptions.setNumReplicas(Integer.parseInt(kuduConfig.getNumReplicas()));
                kuduConfig.kuduContext().createTable(kuduTableName, schema,
                        JavaConverters.asScalaIteratorConverter(kuduConfig.getPrimaryKey().iterator()).asScala().toSeq()
                        , createTableOptions);
                return true;
            } catch (Exception ex) {
               throw new DataAngosException(DataAngosErrorCode.KUDU_TABLE_CREATE_ERROR);
            }
        } else {
            String info = "Error in creating table, as table already exists with" + kuduTableName;
            logger.info(info);
            return false;
        }
    }
}