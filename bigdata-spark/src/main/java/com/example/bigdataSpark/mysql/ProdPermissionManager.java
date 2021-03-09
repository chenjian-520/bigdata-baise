package com.example.bigdataSpark.mysql;

import com.example.bigdataSpark.mysql.entity.DBConnectionInfo;
import java.io.Serializable;


public class ProdPermissionManager implements PermissionManager, Serializable {

    public ProdPermissionManager() {
    }

    public String getUserPermission(String dpUserid) {
        return "hadoop";
    }

    public DBConnectionInfo getMysqlInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setPassword("Foxconn!@34");
        dbConnectionInfo.setUrl("jdbc:mysql://dpbusinessdb:3306/dp_ads?useSSL=false&allowMultiQueries=true");
        dbConnectionInfo.setUsername("dp_ads");
        return dbConnectionInfo;
    }

    public DBConnectionInfo getSqlserverInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setPassword("Foxconn!@34");
        dbConnectionInfo.setUrl("jdbc:sqlserver://dpbusinesssqlserverdb:3000;databaseName=dp_ads;loginTimeout=90;");
        dbConnectionInfo.setUsername("dp_ads");
        return dbConnectionInfo;
    }
}
