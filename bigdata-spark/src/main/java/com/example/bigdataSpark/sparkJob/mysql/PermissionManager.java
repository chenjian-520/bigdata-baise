package com.example.bigdataSpark.sparkJob.mysql;

import com.example.bigdataSpark.sparkJob.mysql.entity.DBConnectionInfo;
import org.apache.hadoop.conf.Configuration;

public interface PermissionManager {

    DBConnectionInfo getMysqlInfo();

    DBConnectionInfo getSqlserverInfo();

    String getRootHdfsUri();

    Configuration initialHdfsSecurityContext();
}

