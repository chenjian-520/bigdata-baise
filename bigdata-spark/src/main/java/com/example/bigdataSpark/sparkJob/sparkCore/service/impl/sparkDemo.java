package com.example.bigdataSpark.sparkJob.sparkCore.service.impl;

import com.example.bigdataSpark.sparkJob.hdfs.SystemFile;
import com.example.bigdataSpark.sparkJob.mysql.DPMysql;
import com.example.bigdataSpark.sparkJob.SparkApp;
import com.example.bigdataSpark.sparkJob.sparkCore.service.sparkService;
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

        SparkSession session = SparkApp.getSession();
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