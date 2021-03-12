package com.ict.bigdata.spark.job.common;

import com.ict.bigdata.spark.job.SparkApp;
import com.ict.bigdata.spark.job.mysql.DPMysql;
import com.ict.bigdata.spark.job.mysql.entity.DBConnectionInfo;
import com.ict.bigdata.spark.job.sparkStreaming.domain.DPKafkaInfo;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;


public class ProdPermissionManager implements PermissionManager, Serializable {

    private static Properties prop;
    private static final Logger logger = LoggerFactory.getLogger(DPMysql.class);

    public ProdPermissionManager() {
        init();
    }

    private static void init() {
        try (InputStream propFile = DPMysql.class.getResource("ProPermissionManager.properties").openStream()) {
            prop.load(new InputStreamReader(propFile, StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("mysql init exception");
        }
    }

    public DBConnectionInfo getMysqlInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setUrl(prop.getProperty("mysqlUrl"));
        dbConnectionInfo.setUsername(prop.getProperty("mysqlUsername"));
        dbConnectionInfo.setPassword(prop.getProperty("mysqlPassword"));
        return dbConnectionInfo;
    }

    public DBConnectionInfo getSqlserverInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setUrl(prop.getProperty("serverSqlUrl"));
        dbConnectionInfo.setUsername(prop.getProperty("serverUsername"));
        dbConnectionInfo.setPassword(prop.getProperty("serverPassword"));
        return dbConnectionInfo;
    }

    public String getRootHdfsUri() {
        return prop.getProperty("hdfsUri");
    }

    public Configuration initialHdfsSecurityContext() {
        Configuration config = new Configuration();
        return config;
    }

    @Override
    public DPKafkaInfo initialKafkaSecurityContext() {
        DPKafkaInfo dpKafkaInfo = SparkApp.getDPKafkaInfo();
        dpKafkaInfo.setServerUrl(prop.getProperty("kafkaServerUrl"));
        return dpKafkaInfo;
    }
}
