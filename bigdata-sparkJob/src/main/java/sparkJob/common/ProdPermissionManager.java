package sparkJob.common;


import org.apache.hadoop.conf.Configuration;
import sparkJob.SparkApp;
import sparkJob.mysql.entity.DBConnectionInfo;
import sparkJob.sparkStreaming.domain.DPKafkaInfo;
import java.io.Serializable;


public class ProdPermissionManager implements PermissionManager, Serializable {

    public ProdPermissionManager() {
    }

    public String getUserPermission(String dpUserid) {
        return "hadoop";
    }

    public DBConnectionInfo getMysqlInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setPassword("root");
        dbConnectionInfo.setUrl("jdbc:mysql://127.0.0.1:3306/bigdata?useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai");
        dbConnectionInfo.setUsername("root");
        return dbConnectionInfo;
    }

    public DBConnectionInfo getSqlserverInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setPassword("root");
        dbConnectionInfo.setUrl("jdbc:sqlserver://127.0.0.1:3000;databaseName=bigdata;loginTimeout=90;");
        dbConnectionInfo.setUsername("root");
        return dbConnectionInfo;
    }

    public String getRootHdfsUri() {
        return "hdfs://hadoop:8020";
    }

    public Configuration initialHdfsSecurityContext() {
        Configuration config = new Configuration();
        return config;
    }

    @Override
    public DPKafkaInfo initialKafkaSecurityContext() {
        DPKafkaInfo dpKafkaInfo = SparkApp.getDPKafkaInfo();
        dpKafkaInfo.setServerUrl("127.0.0.1:9092");
        return dpKafkaInfo;
    }
}
