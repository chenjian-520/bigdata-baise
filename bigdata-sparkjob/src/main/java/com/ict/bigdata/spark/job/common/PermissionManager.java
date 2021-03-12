package com.ict.bigdata.spark.job.common;

import org.apache.hadoop.conf.Configuration;
import com.ict.bigdata.spark.job.mysql.entity.DBConnectionInfo;
import com.ict.bigdata.spark.job.sparkStreaming.domain.DPKafkaInfo;

public interface PermissionManager {

    DBConnectionInfo getMysqlInfo();

    String getRootHdfsUri();

    Configuration initialHdfsSecurityContext();

    DPKafkaInfo initialKafkaSecurityContext();
}

