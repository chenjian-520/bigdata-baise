package com.example.bigdataSpark.sparkJob.service.impl;

import com.example.bigdataSpark.hdfs.HdfsClient;
import com.example.bigdataSpark.hdfs.SystemFile;
import com.example.bigdataSpark.mysql.DPMysql;
import com.example.bigdataSpark.sparkJob.domain.StreamingDataGzipOutputFormat;
import com.example.bigdataSpark.sparkJob.domain.mysqlBean.User;
import com.example.bigdataSpark.sparkJob.service.sparkService;
import com.example.bigdataSpark.sparkJob.sparkApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class sparkDemo implements sparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        //读写本地文件
        JavaRDD<String> stringJavaRDD = SystemFile.readSystemFile("C:\\Users\\issuser\\Desktop\\笔记本\\笔记bigdata.txt", 1);
//        stringJavaRDD.saveAsTextFile("D:\\1");
//        SystemFile.saveSystemFile(stringJavaRDD,"C:\\Users\\issuser\\Desktop\\1");

        JavaPairRDD<String, String> stringStringJavaPairRDD = stringJavaRDD.keyBy(r -> r);

        // spark 写 hdfs
//        Configuration hadoopConf = stringStringJavaPairRDD.context().hadoopConfiguration();
//        hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true");
//        hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
//        hadoopConf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
//        stringStringJavaPairRDD.saveAsNewAPIHadoopFile("D:\\1", CombineTextInputFormat.class, CombineTextInputFormat.class, StreamingDataGzipOutputFormat.class);

        JavaRDD<Row> rowJavaRDD = DPMysql.rddRead("(select * from bigdata.user where 1=1) tmp");

        SparkSession session = sparkApp.getSession();
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(fields);
        Dataset<Row> dataFrame = session.createDataFrame(rowJavaRDD, structType);
        dataFrame.createOrReplaceTempView("chenjian");
        Dataset<Row> sql = session.sql("select * from chenjian");
        sql.show();
        DPMysql.commonOdbcWriteBatch("user",sql);

        return null;
    }
}
