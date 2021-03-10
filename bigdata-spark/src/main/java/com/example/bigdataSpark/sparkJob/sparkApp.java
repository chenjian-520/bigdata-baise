package com.example.bigdataSpark.sparkJob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.example.bigdataSpark.mysql.PermissionManager;
import com.example.bigdataSpark.sparkClick.service.sparkClient;
import com.example.bigdataSpark.sparkJob.service.sparkService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.slf4j.LoggerFactory;
import scala.Serializable;


public class sparkApp implements Serializable {

     public static CollectionAccumulator<SparkSession> sessionBroadcast;

     public static CollectionAccumulator<JavaSparkContext> contextBroadCast;

     public static PermissionManager permissionManager;

     public static Broadcast<PermissionManager> permissionBroadcast;

     public static void main(String[] args) throws Exception{
         Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
         Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
         Logger.getLogger("org.project-spark").setLevel(Level.WARN);
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
         SparkSession sparkSession = sessionBroadcast.value().get(0);
         return sparkSession;
     }

    public static PermissionManager getDpPermissionManager() {
        return (PermissionManager)permissionBroadcast.value();
    }

}
