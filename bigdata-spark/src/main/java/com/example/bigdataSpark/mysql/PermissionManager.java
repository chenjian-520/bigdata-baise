package com.example.bigdataSpark.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import com.example.bigdataSpark.mysql.entity.DBConnectionInfo;
import com.example.bigdataSpark.mysql.entity.RDBConnetInfo;
import org.apache.hadoop.conf.Configuration;

public interface PermissionManager {

    DBConnectionInfo getMysqlInfo();

    DBConnectionInfo getSqlserverInfo();

}

