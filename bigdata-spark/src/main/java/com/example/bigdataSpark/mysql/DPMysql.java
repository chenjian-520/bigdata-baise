
package com.example.bigdataSpark.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.example.bigdataSpark.mysql.entity.DBConnectionInfo;
import com.example.bigdataSpark.mysql.entity.DlFunction;

import com.example.bigdataSpark.mysql.entity.RDBConnetInfo;
import com.example.bigdataSpark.sparkJob.sparkApp;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author cj
 * @Description mysql操作类
 * @timestamp 2021/03/09
 */
public class DPMysql {
    private static final Logger log = LoggerFactory.getLogger(DPMysql.class);
    private static final String ERROR_MES = "未找到数据库连接配置信息";

    public DPMysql() {
    }

    /**
     * 读取mysql数据
     *
     * @param dburl      数据库连接
     * @param dbuser     数据库账号
     * @param dbPassword 数据库密码
     * @param query      查询语句
     */
    protected static JavaRDD<Row> rddRead(String dburl, String dbuser, String dbPassword, String query) {
        SparkSession sparkSession = sparkApp.getSession();
        Dataset<Row> dataset = sparkSession.read().format("jdbc").option("url", dburl).option("driver", "com.mysql.jdbc.Driver").option("user", dbuser).option("password", dbPassword).option("query", query).load();
        return dataset.toJavaRDD();
    }

    /**
     * @param query 查询语句
     *              查询默认db中的数据
     */
    public static JavaRDD<Row> rddRead(String query) {
        DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
        return rddRead(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), query);
    }

    public static JavaRDD<Row> rddRead(RDBConnetInfo rdbConnetInfo, String query) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            return rddRead(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), query);
        }
    }

    /**
     * 批量插入数据到mysql，使用odbc写入，适合海量数据写入
     */
    protected static void commonOdbcWriteBatch(String dburl, String dbuser, String dbPassword, String mysqltablename, JavaRDD<Row> insertdata, HashMap<String, StructField> dbcolums, StructType rowAgeNameSchema) {
        SparkSession sparkSession = sparkApp.getSession();
        Dataset<Row> insertdataDs = sparkSession.createDataFrame(insertdata, rowAgeNameSchema);
        insertdataDs.foreachPartition((iterator) -> {
            Driver mysqldriver = (Driver)Class.forName("com.mysql.jdbc.Driver").newInstance();
            Properties dbpro = new Properties();
            dbpro.put("user", dbuser);
            dbpro.put("password", dbPassword);
            Connection con = null;

            try {
                con = mysqldriver.connect(dburl, dbpro);
                con.setAutoCommit(true);
                String sql = "INSERT INTO " + mysqltablename + "(  ";
                String values = "values ( ";
                Iterator iter = dbcolums.entrySet().iterator();

                while(iter.hasNext()) {
                    Map.Entry entry = (Map.Entry)iter.next();
                    String key = entry.getKey().toString();
                    if (!iter.hasNext()) {
                        sql = sql + key + " ) ";
                        values = values + "? ) ";
                    } else {
                        sql = sql + key + ",";
                        values = values + "?,";
                    }
                }

                sql = sql + values;
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                AtomicInteger cout = new AtomicInteger(0);

                while(iterator.hasNext()) {
                    Row p = (Row)iterator.next();
                    iter = dbcolums.entrySet().iterator();

                    for(int i = 0; iter.hasNext(); ++i) {
                        Map.Entry ent = (Map.Entry)iter.next();
                        StructField structField = (StructField)ent.getValue();
                        if (structField.dataType().equals(DataTypes.IntegerType)) {
                            preparedStatement.setInt(i + 1, (Integer)p.getAs(ent.getKey().toString()));
                        } else if (structField.dataType().equals(DataTypes.BooleanType)) {
                            preparedStatement.setBoolean(i + 1, (Boolean)p.getAs(ent.getKey().toString()));
                        } else if (structField.dataType().equals(DataTypes.LongType)) {
                            preparedStatement.setLong(i + 1, (Long)p.getAs(ent.getKey().toString()));
                        } else if (structField.dataType().equals(DataTypes.DoubleType)) {
                            preparedStatement.setDouble(i + 1, (Double)p.getAs(ent.getKey().toString()));
                        } else if (structField.dataType().equals(DataTypes.FloatType)) {
                            preparedStatement.setFloat(i + 1, (Float)p.getAs(ent.getKey().toString()));
                        } else if (structField.dataType().equals(DataTypes.ShortType)) {
                            preparedStatement.setShort(i + 1, (Short)p.getAs(ent.getKey().toString()));
                        } else {
                            preparedStatement.setString(i + 1, (String)p.getAs(ent.getKey().toString()));
                        }
                    }

                    preparedStatement.addBatch();
                    if (cout.addAndGet(1) >= 2000) {
                        cout.set(0);
                        preparedStatement.executeBatch();
                    }
                }

                try {
                    preparedStatement.executeBatch();
                } catch (Exception var22) {
                    System.out.println(var22.getMessage());
                } finally {
                    preparedStatement.close();
                    con.close();
                }
            } catch (Exception var24) {
                System.out.println("mysql connect error:" + var24.getMessage());
                log.error("mysql connect error:" + var24.getMessage(), var24);
            }

        });
    }

    public static void commonOdbcWriteBatch(String mysqltablename, JavaRDD<Row> insertdata, HashMap<String, StructField> dbcolums, StructType rowSchema) {
        DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
        commonOdbcWriteBatch(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), mysqltablename, insertdata, dbcolums, rowSchema);
    }

    /**
     * 直接使用dataset写入，无法使用在海量数据
     */
    protected static void commonOdbcWriteBatch(final String dburl, final String dbuser, final String dbPassword, final String mysqltablename, Dataset ds) {
        StructType structType = ds.schema();
        final HashMap<String, DataType> fields = new HashMap();
        StructField[] var7 = structType.fields();
        int var8 = var7.length;

        for(int var9 = 0; var9 < var8; ++var9) {
            StructField structField = var7[var9];
            if (!"rowkey".equals(structField.name())) {
                fields.put(structField.name(), structField.dataType());
            }
        }

        ds.foreachPartition(new ForeachPartitionFunction() {
            public void call(Iterator iterator) throws Exception {
                Driver mysqldriver = (Driver)Class.forName("com.mysql.jdbc.Driver").newInstance();
                Properties dbpro = new Properties();
                dbpro.put("user", dbuser);
                dbpro.put("password", dbPassword);
                Connection con = null;

                try {
                    con = mysqldriver.connect(dburl, dbpro);
                    con.setAutoCommit(true);
                    String sql = "INSERT INTO " + mysqltablename + "(  ";
                    String values = "values ( ";
                    Iterator iter = fields.entrySet().iterator();

                    while(iter.hasNext()) {
                        Map.Entry entryx = (Map.Entry)iter.next();
                        String key = entryx.getKey().toString();
                        if (!iter.hasNext()) {
                            sql = sql + key + " ) ";
                            values = values + "? ) ";
                        } else {
                            sql = sql + key + ",";
                            values = values + "?,";
                        }
                    }

                    sql = sql + values;
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    AtomicInteger cout = new AtomicInteger(0);

                    while(iterator.hasNext()) {
                        int i = 0;
                        GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema)iterator.next();
                        Iterator var12 = fields.entrySet().iterator();

                        while(var12.hasNext()) {
                            Map.Entry entry = (Map.Entry)var12.next();

                            try {
                                if (((DataType)entry.getValue()).equals(DataTypes.IntegerType)) {
                                    preparedStatement.setInt(i + 1, (Integer)genericRowWithSchema.getAs((String)entry.getKey()));
                                } else if (((DataType)entry.getValue()).equals(DataTypes.BooleanType)) {
                                    preparedStatement.setBoolean(i + 1, (Boolean)genericRowWithSchema.getAs((String)entry.getKey()));
                                } else if (((DataType)entry.getValue()).equals(DataTypes.LongType)) {
                                    preparedStatement.setLong(i + 1, (Long)genericRowWithSchema.getAs((String)entry.getKey()));
                                } else if (((DataType)entry.getValue()).equals(DataTypes.DoubleType)) {
                                    preparedStatement.setDouble(i + 1, (Double)genericRowWithSchema.getAs((String)entry.getKey()));
                                } else if (((DataType)entry.getValue()).equals(DataTypes.FloatType)) {
                                    preparedStatement.setFloat(i + 1, (Float)genericRowWithSchema.getAs((String)entry.getKey()));
                                } else if (((DataType)entry.getValue()).equals(DataTypes.ShortType)) {
                                    preparedStatement.setShort(i + 1, (Short)genericRowWithSchema.getAs((String)entry.getKey()));
                                } else {
                                    preparedStatement.setString(i + 1, genericRowWithSchema.getAs((String)entry.getKey()).toString());
                                }

                                ++i;
                            } catch (SQLException var22) {
                                log.error("=====spark sdk==========数据插入发现异常:" + var22.getMessage());
                            }
                        }

                        preparedStatement.addBatch();
                        if (cout.addAndGet(1) >= 2000) {
                            cout.set(0);
                            preparedStatement.executeBatch();
                        }
                    }

                    try {
                        preparedStatement.executeBatch();
                    } catch (Exception var20) {
                        System.out.println(var20.getMessage());
                    } finally {
                        preparedStatement.close();
                        con.close();
                    }
                } catch (Exception var23) {
                }

            }
        });
    }

    public static void commonOdbcWriteBatch(String mysqltablename, Dataset ds) {
        DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
        commonOdbcWriteBatch(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), mysqltablename, ds);
    }

    public static void commonOdbcWriteBatch(RDBConnetInfo rdbConnetInfo, String mysqltablename, Dataset ds) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            commonOdbcWriteBatch(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), mysqltablename, ds);
        }
    }

    public static void commonOdbcUpdateBatch(RDBConnetInfo rdbConnetInfo, String mysqltablename, JavaRDD<Row> updateRdd, HashMap<String, StructField> dbcolums, StructType rowSchema, DlFunction<Row, String> whereFunction) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            commonOdbcUpdateBatch(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), mysqltablename, updateRdd, dbcolums, rowSchema, whereFunction);
        }
    }


    public static void commonOdbcUpdateBatch(String mysqltablename, JavaRDD<Row> updateRdd, HashMap<String, StructField> dbcolums, StructType rowSchema, DlFunction<Row, String> whereFunction) throws Exception {
        if (whereFunction != null && !StringUtils.isBlank(mysqltablename)) {
            DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
            commonOdbcUpdateBatch(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), mysqltablename, updateRdd, dbcolums, rowSchema, whereFunction);
        } else {
            throw new RuntimeException("mysql table name/where 条件回调不能为空!");
        }
    }
// 使用样例
//        DPMysql.commonOdbcUpdateBatch("dp_ads", "dpm_ads_production_site_kpi_month", l6OeeMonth.toJavaRDD(), schemaList, l6OeeMonth.schema(), new DlFunction<Row, String>() {
//            @Override
//            public String apply(Row row) {
//                StringBuffer stringBuffer = new StringBuffer();
//                String append = stringBuffer.append("month_id = ").append("'").append(formatYYYYMM(getMonth())).append("'")
//                        .append("and site_code = ").append("'").append("WH").append("'")
//                        .append("and level_code = ").append("'").append("L6").append("'").toString();
//                return append;
//            }
//        });

    public static void commonOdbcWriteBatch(RDBConnetInfo rdbConnetInfo, String mysqltablename, JavaRDD<Row> insertdata, HashMap<String, StructField> dbcolums, StructType rowSchema) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            commonOdbcWriteBatch(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), mysqltablename, insertdata, dbcolums, rowSchema);
        }
    }

    protected static void commonOdbcUpdateBatch(String dburl, String dbuser, String dbPassword, String mysqltablename, JavaRDD<Row> updateRdd, HashMap<String, StructField> dbcolums, StructType rowSchema, DlFunction<Row, String> whereFunction) throws Exception {
        if (whereFunction != null && !StringUtils.isBlank(mysqltablename)) {
            SparkSession sparkSession = sparkApp.getSession();
            Dataset<Row> updatedataDs = sparkSession.createDataFrame(updateRdd, rowSchema);
            updatedataDs.foreachPartition((iterator) -> {
                Driver mysqldriver = (Driver)Class.forName("com.mysql.jdbc.Driver").newInstance();
                Properties dbpro = new Properties();
                dbpro.put("user", dbuser);
                dbpro.put("password", dbPassword);
                Connection con = null;

                try {
                    con = mysqldriver.connect(dburl, dbpro);
                    DatabaseMetaData dbmd = con.getMetaData();
                    Statement statement = con.createStatement();
                    if (!dbmd.supportsBatchUpdates()) {
                        log.error("数据库连接不支持批量更新，请调整DB连接！");
                    } else {
                        con.setAutoCommit(false);

                        while(iterator.hasNext()) {
                            Row p = (Row)iterator.next();
                            StringBuilder sqlBuilder = new StringBuilder("UPDATE " + mysqltablename + " set ");
                            Iterator iter = dbcolums.entrySet().iterator();

                            while(iter.hasNext()) {
                                Map.Entry entry = (Map.Entry)iter.next();
                                String key = entry.getKey().toString();
                                StructField structField = (StructField)entry.getValue();
                                String rddkey;
                                Integer dvxxxxx;
                                Boolean dv;
                                Long dvx;
                                Double dvxx;
                                Float dvxxx;
                                String dvxxxx;
                                if (!iter.hasNext()) {
                                    sqlBuilder.append("`" + key + "`=");
                                    rddkey = structField.name();
                                    if (structField.dataType().equals(DataTypes.IntegerType)) {
                                        dvxxxxx = Integer.parseInt(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvxxxxx);
                                    } else if (structField.dataType().equals(DataTypes.BooleanType)) {
                                        dv = Boolean.parseBoolean(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dv);
                                    } else if (structField.dataType().equals(DataTypes.LongType)) {
                                        dvx = Long.parseLong(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvx);
                                    } else if (structField.dataType().equals(DataTypes.DoubleType)) {
                                        dvxx = Double.parseDouble(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvxx);
                                    } else if (structField.dataType().equals(DataTypes.FloatType)) {
                                        dvxxx = Float.parseFloat(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvxxx);
                                    } else {
                                        dvxxxx = p.getAs(rddkey).toString();
                                        sqlBuilder.append("'" + dvxxxx + "'");
                                    }

                                    sqlBuilder.append("  where " + (String)whereFunction.apply(p));
                                } else {
                                    key = entry.getKey().toString();
                                    sqlBuilder.append("`" + key + "`=");
                                    rddkey = structField.name();
                                    p.getAs(rddkey).toString();
                                    if (structField.dataType().equals(DataTypes.IntegerType)) {
                                        dvxxxxx = Integer.parseInt(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvxxxxx + ",");
                                    } else if (structField.dataType().equals(DataTypes.BooleanType)) {
                                        dv = Boolean.parseBoolean(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dv + ",");
                                    } else if (structField.dataType().equals(DataTypes.LongType)) {
                                        dvx = Long.parseLong(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvx + ",");
                                    } else if (structField.dataType().equals(DataTypes.DoubleType)) {
                                        dvxx = Double.parseDouble(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvxx + ",");
                                    } else if (structField.dataType().equals(DataTypes.FloatType)) {
                                        dvxxx = Float.parseFloat(p.getAs(rddkey).toString());
                                        sqlBuilder.append(dvxxx + ",");
                                    } else {
                                        dvxxxx = p.getAs(rddkey).toString();
                                        sqlBuilder.append("'" + dvxxxx + "',");
                                    }
                                }
                            }

                            statement.addBatch(sqlBuilder.toString());
                            log.info("===更新sql为=====" + sqlBuilder.toString());
                        }

                        statement.executeBatch();
                        con.commit();
                        statement.close();
                    }
                } catch (Exception var23) {
                    System.out.println("sql server connect error:" + var23.getMessage());
                    log.error("sql server connect error:" + var23.getMessage(), var23);
                } finally {
                    con.close();
                }

            });
        } else {
            throw new RuntimeException("mysql table name/where 条件回调不能为空!");
        }
    }

    public static void commonOdbcDeleteBatch(RDBConnetInfo rdbConnetInfo, String mysqltablename, JavaRDD<Row> deleteRdd, StructType rowSchema, DlFunction<Row, String> whereFunction) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            commonOdbcDeleteBatch(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), mysqltablename, deleteRdd, rowSchema, whereFunction);
        }
    }

    protected static void commonOdbcDeleteBatch(String dburl, String dbuser, String dbPassword, String mysqltablename, JavaRDD<Row> deleteRdd, StructType rowSchema, DlFunction<Row, String> whereFunction) throws Exception {
        SparkSession sparkSession = sparkApp.getSession();
        Dataset<Row> insertdataDs = sparkSession.createDataFrame(deleteRdd, rowSchema);
        insertdataDs.foreachPartition((iterator) -> {
            Driver mysqldriver = (Driver)Class.forName("com.mysql.jdbc.Driver").newInstance();
            Properties dbpro = new Properties();
            dbpro.put("user", dbuser);
            dbpro.put("password", dbPassword);
            Connection con = null;

            try {
                con = mysqldriver.connect(dburl, dbpro);
                DatabaseMetaData dbmd = con.getMetaData();
                Statement statement = con.createStatement();
                if (!dbmd.supportsBatchUpdates()) {
                    log.error("数据库连接不支持批量更新，请调整DB连接！");
                } else {
                    con.setAutoCommit(false);
                    StringBuilder sqlBuilder = new StringBuilder(" DELETE FROM " + mysqltablename + " ");

                    while(iterator.hasNext()) {
                        Row p = (Row)iterator.next();
                        sqlBuilder.append("  where " + (String)whereFunction.apply(p));
                        statement.addBatch(sqlBuilder.toString());
                        log.info("===删除的sql为=====" + sqlBuilder.toString());
                    }

                    statement.executeBatch();
                    con.commit();
                    statement.close();
                }
            } catch (Exception var16) {
                log.error("sql server connect error:" + var16.getMessage(), var16);
             } finally {
                con.close();
            }

        });
    }

    public static void commonOdbcDeleteBatch(String mysqltablename, JavaRDD<Row> deleteRdd, StructType rowSchema, DlFunction<Row, String> whereFunction) throws Exception {
        DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
        commonOdbcDeleteBatch(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), mysqltablename, deleteRdd, rowSchema, whereFunction);
    }

    public static void commonOdbcExecuteSql(RDBConnetInfo rdbConnetInfo, List<String> sqls) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            commonOdbcExecuteSql(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), sqls);
        }
    }

    protected static void commonOdbcExecuteSql(String dburl, String dbuser, String dbPassword, List<String> sqls) throws Exception {
        Driver mysqldriver = (Driver)Class.forName("com.mysql.jdbc.Driver").newInstance();
        Properties dbpro = new Properties();
        dbpro.put("user", dbuser);
        dbpro.put("password", dbPassword);
        Connection con = mysqldriver.connect(dburl, dbpro);
        Throwable var7 = null;

        try {
            Statement statement = con.createStatement();
            Iterator var9 = sqls.iterator();

            while(var9.hasNext()) {
                String sql = (String)var9.next();
                statement.addBatch(sql);
            }

            statement.executeBatch();
            con.commit();
        } catch (Throwable var18) {
            var7 = var18;
            throw var18;
        } finally {
            if (con != null) {
                if (var7 != null) {
                    try {
                        con.close();
                    } catch (Throwable var17) {
                        var7.addSuppressed(var17);
                    }
                } else {
                    con.close();
                }
            }

        }
    }

    public static void commonOdbcExecuteSql(List<String> sqls) throws Exception {
        DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
        commonOdbcExecuteSql(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), sqls);
    }

    protected static void commonDatasetWriteBatch(String dburl, String dbuser, String dbPassword, String tablename, JavaRDD<Row> insertdata, List<StructField> fieldList, SaveMode saveMode) {
        SparkSession sparkSession = sparkApp.getSession();
        StructType rowAgeNameSchema = DataTypes.createStructType(fieldList);
        Dataset<Row> insertdataDs = sparkSession.createDataFrame(insertdata, rowAgeNameSchema);
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbuser);
        connectionProperties.put("password", dbPassword);
        insertdataDs.write().mode(saveMode).jdbc(dburl, tablename, connectionProperties);
    }

    public static void commonDatasetWriteBatch(String tablename, JavaRDD<Row> insertdata, List<StructField> fieldList, SaveMode saveMode) {
        DBConnectionInfo dbConnectionInfo = sparkApp.getDpPermissionManager().getMysqlInfo();
        commonDatasetWriteBatch(dbConnectionInfo.getUrl(), dbConnectionInfo.getUsername(), dbConnectionInfo.getPassword(), tablename, insertdata, fieldList, saveMode);
    }

    public static void commonDatasetWriteBatch(RDBConnetInfo rdbConnetInfo, String tablename, JavaRDD<Row> insertdata, List<StructField> fieldList, SaveMode saveMode) throws Exception {
        if (rdbConnetInfo == null) {
            throw new Exception("未找到数据库连接配置信息");
        } else {
            commonDatasetWriteBatch(rdbConnetInfo.getDbUrl(), rdbConnetInfo.getDbUsername(), rdbConnetInfo.getDbPassword(), tablename, insertdata, fieldList, saveMode);
        }
    }

}
