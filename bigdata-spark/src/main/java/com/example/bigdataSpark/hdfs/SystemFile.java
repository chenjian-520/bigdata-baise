package com.example.bigdataSpark.hdfs;

import com.example.bigdataSpark.sparkJob.sparkApp;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Arrays;

public class SystemFile {

    /**
     * 默认是从hdfs读取文件，也可以指定sc.textFile("路径").在路径前面加上hdfs://表示从hdfs文件系统上读
     * 本地文件读取 sc.textFile("路径").在路径前面加上file:// 表示从本地文件系统读，如file:///home/user/spark/README.md
     * 读取Text文件，使用readHdfsFile
     */
    public static JavaRDD<String> readSystemFile(String path, int partitionCount) {
        JavaSparkContext sparkContext = sparkApp.contextBroadCast.value().get(0);
        JavaRDD<String> txtData =
                sparkContext.textFile(path, partitionCount).persist(StorageLevel.MEMORY_AND_DISK());
        return txtData;
    }

    public static void saveSystemFile(JavaRDD rdd, String path) {
        sparkApp.getSession().sparkContext().hadoopConfiguration().
                set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");
        rdd.repartition(1).saveAsTextFile(path);
    }
}
