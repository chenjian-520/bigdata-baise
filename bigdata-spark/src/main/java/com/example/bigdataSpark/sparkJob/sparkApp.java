package com.example.bigdataSpark.sparkJob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.example.bigdataSpark.sparkJob.service.sparkService;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

public class sparkApp implements Serializable {
    private  static  final Logger DEBUG_LOGGER= LoggerFactory.getLogger(sparkApp.class);

     public static CollectionAccumulator<SparkSession> sessionBroadcast;

     public static CollectionAccumulator<JavaSparkContext> contextBroadCast;

     public static Broadcast<JSONObject> appParamBroadcast;

     public static void main(String[] args) throws Exception{
         //获取arg参数
         String arg=args[0];
         JSONObject appParam = JSON.parseObject(arg);
         SparkSession sparkSession =null;
         SparkContext sparkContext  =null;
         try{
             sparkSession = SparkSession.builder().
                     appName(appParam.getString("appName"))
                     .master("local[*]").config("spark.serializer","org.apache.spark.serializer.krgoserializer")
                     .getOrCreate();

             sparkContext =sparkSession.sparkContext();
             JavaSparkContext javaSparkContext =JavaSparkContext.fromSparkContext(sparkContext);

             //必要广播变量
             contextBroadCast =sparkContext.collectionAccumulator("jcontext");
             contextBroadCast.add(javaSparkContext);
             sessionBroadcast =sparkContext.collectionAccumulator("sparksession");
             sessionBroadcast.add(sparkSession);

             Class<?> serviceclazz =Class.forName(appParam.getString("sericeName"));
             sparkService sparkservice =(sparkService) serviceclazz.newInstance();
             sparkservice.execute();

         }finally {
             if(sparkContext !=null){
                 sparkContext.stop();
             }
             if(sparkSession!=null){
                 sparkSession.stop();
             }
         }
     }

}
