package com.example.bigdataSpark.sparkJob.common;


import org.apache.hadoop.conf.Configuration;
import com.example.bigdataSpark.sparkJob.mysql.entity.DBConnectionInfo;
import com.example.bigdataSpark.sparkJob.sparkStreaming.domain.DPKafkaInfo;

public interface PermissionManager {

    DBConnectionInfo getMysqlInfo();

    DBConnectionInfo getSqlserverInfo();

    String getRootHdfsUri();

    Configuration initialHdfsSecurityContext();

    DPKafkaInfo initialKafkaSecurityContext();
}

