package com.example.bigdataSpark.sparkJob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.example.bigdataSpark.mysql.PermissionManager;
import com.example.bigdataSpark.mysql.entity.RDBConnetInfo;
import com.example.bigdataSpark.sparkJob.service.sparkService;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.util.Map;

public class sparkApp implements Serializable {

     public static CollectionAccumulator<SparkSession> sessionBroadcast;

     public static CollectionAccumulator<JavaSparkContext> contextBroadCast;

     private static final Logger log = LoggerFactory.getLogger(sparkApp.class);

     public static PermissionManager permissionManager;

     public static Broadcast<PermissionManager> permissionBroadcast;

     public static void main(String[] args) throws Exception{
         //获取arg参数
         String arg=args[0];
         JSONObject appParam = JSON.parseObject(arg);
         SparkSession sparkSession =null;
         SparkContext sparkContext  =null;
         try{
             sparkSession = SparkSession.builder().
                     appName(appParam.getString("appName"))
                     .master("local[*]")
                     .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                     .getOrCreate();

             sparkContext =sparkSession.sparkContext();
             JavaSparkContext javaSparkContext =JavaSparkContext.fromSparkContext(sparkContext);

             //必要广播变量
             contextBroadCast =sparkContext.collectionAccumulator("jcontext");
             contextBroadCast.add(javaSparkContext);
             sessionBroadcast =sparkContext.collectionAccumulator("sparksession");
             sessionBroadcast.add(sparkSession);

             permissionManager = (PermissionManager)Class.forName("com.example.bigdataSpark.mysql.ProdPermissionManager").newInstance();
             permissionBroadcast = javaSparkContext.broadcast(permissionManager);
             log.info("SparkSDK==LOG==获取rdb连接配置成功!");

             Class<?> serviceclazz =Class.forName(appParam.getString("sericeName"));
             sparkService sparkservice =(sparkService) serviceclazz.newInstance();
             sparkservice.execute(appParam);

         }finally {
             if(sparkContext !=null){
                 sparkContext.stop();
             }
             if(sparkSession!=null){
                 sparkSession.stop();
             }
         }
     }

     public static SparkSession getSession(){
         return sessionBroadcast.value().get(0);
     }

    public static PermissionManager getDpPermissionManager() {
        return (PermissionManager)permissionBroadcast.value();
    }

}
