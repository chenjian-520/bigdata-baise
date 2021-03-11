package com.example.bigdataSpark.sparkJob.common;

import com.example.bigdataSpark.sparkJob.mysql.entity.DBConnectionInfo;
import com.example.bigdataSpark.sparkJob.sparkStreaming.domain.DPKafkaInfo;
import org.apache.hadoop.conf.Configuration;

public interface PermissionManager {

    DBConnectionInfo getMysqlInfo();

    DBConnectionInfo getSqlserverInfo();

    String getRootHdfsUri();

    Configuration initialHdfsSecurityContext();

    DPKafkaInfo initialKafkaSecurityContext();
}

