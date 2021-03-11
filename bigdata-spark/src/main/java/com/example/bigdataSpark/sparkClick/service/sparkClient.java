package com.example.bigdataSpark.sparkClick.service;


import com.example.bigdataSpark.sparkClick.constants.SparkConstants;
import com.example.bigdataSpark.sparkClick.domain.sparkResult;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public class sparkClient {

    /**
    * 日志
    * */
     private static final Logger logger = LoggerFactory.getLogger(sparkClient.class);
    /**
     * 单例
     * */
    private  static  final sparkClient sigle = new sparkClient();

    /**
     * 属性
     * */
    private static Properties prop;

    /**
     * 私有化构造器
     * */
    private sparkClient(){ init(); };

    /**
     * 获取实例
     * */
    public static  sparkClient getInstance(){return sigle;};

    /**
     * 启动SparkApp
     * */
    public static String startSparkApp(String appName,String json,Function<sparkResult,Void> callback) throws IOException, InterruptedException {
       //日志输出
        logger.info("startSparkApp.Appname",appName);
        HashMap env = new HashMap();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在
        //SparkAppHandle handle = new SparkLauncher(env)
        SparkLauncher launcher= new SparkLauncher()
                .setSparkHome(prop.getProperty("sparkHome"))
                .setAppResource(prop.getProperty("appResource"))
                .setMainClass(prop.getProperty("mainClass"))
                .setMaster(prop.getProperty("master"))
                .setDeployMode(prop.getProperty("deployMode"))
                .setPropertiesFile(prop.getProperty("propertiesFile"))
                .setVerbose(Boolean.parseBoolean(prop.getProperty("verbose")))
                .addAppArgs(new String[] {json});


        SparkAppHandle handle = launcher.startApplication();
         handle.addListener(new SparkAppHandle.Listener() {
            //这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                sparkResult sparkResult = new sparkResult(handle.getAppId());
                if(SparkAppHandle.State.FINISHED.equals(handle.getState())){
                    sparkResult.setState(SparkConstants.App_SUCCESS);
                }

                if (sparkAppHandle.getState().isFinal()) {
                    countDownLatch.countDown();
                }
                System.out.println("state:" + sparkAppHandle.getState().toString());
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                System.out.println("Info:" + sparkAppHandle.getState().toString());
            }

        });

        System.out.println("The task is executing, please wait ....");
        //线程等待任务结束
        countDownLatch.await();
        System.out.println("The task is finished!");
        return handle.getAppId();
    }

    private static void init(){
        try(InputStream propFile = sparkClient.class.getResource("spark-client.properties").openStream()) {
                   prop.load(new InputStreamReader(propFile, StandardCharsets.UTF_8));
        }catch(IOException e){
            logger.error("spark client init exception");
        }
        }
    }

