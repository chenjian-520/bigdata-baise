package com.example.bigdataSpark.sparkJob.service.impl;

import com.example.bigdataSpark.hdfs.HdfsClient;
import com.example.bigdataSpark.hdfs.SystemFile;
import com.example.bigdataSpark.sparkJob.service.sparkService;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

public class sparkDemo implements sparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        Object o = String.valueOf(var.get(""));
        JavaRDD<String> stringJavaRDD = SystemFile.readSystemFile("C:\\Users\\issuser\\Desktop\\笔记本\\笔记bigdata.txt", 1);
        stringJavaRDD.saveAsTextFile("D:\\1");
//        SystemFile.saveSystemFile(stringJavaRDD,"C:\\Users\\issuser\\Desktop\\1");
        return null;
    }
}
