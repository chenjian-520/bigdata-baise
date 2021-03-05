package com.example.bigdataSpark.hdfs;

import com.example.bigdataSpark.sparkJob.sparkApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class HdfsClient {
    /**
     * 日志对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsClient.class);

    /**
     * username 认证principalkey
     */
    private static String PRINCIPAL = "username.client.kerberos.principal";

    /**
     * username 认证keytab.key
     */
    private static String KEYTAB = "username.client.keytab.file";

    /**
     * 单例
     */
    private static final HdfsClient single = new HdfsClient();

    /**
     * HDFS 配置
     */
    private static Configuration conf;

    private static FileSystem fileSystem = null;
    /**
     * spark 属性配置
     */
    private static Properties props;

    /**
     * 私有化构造器
     */
    private HdfsClient() {
        init();
    }

    /**
     * 获取实例
     */
    public static HdfsClient getInstance() {
        return single;
    }

    /**
     * 下载HDFS文件输出到outputstream
     */
    public void downLoadHdfsFile(String path, String fileName, OutputStream outputStream) throws IOException {
        /**
         * 日志输出
         */
        LOGGER.info("download start,path={},fileName={}", path, fileName);

        /**
         * 认证
         */
        authentication();
        /**
         * 获取HDFS文件系统
         */
        FileSystem fileSystem = FileSystem.get(conf);
        /**
         * 获取文件path
         */
        Path filePath = new Path(path, fileName);
        try (FSDataInputStream fsDataInputStream = fileSystem.open(filePath);) {
            byte[] buff = new byte[10];
            int length = -1;
            while (fsDataInputStream.read(buff) != -1) {
                outputStream.write(buff, 0, length);
            }
            LOGGER.info("download finish,path={},fileName={}", path, fileName);
        }
    }

    /**
     * 用户认证
     */
    private void authentication() throws IOException {
        if ("kerberis".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
            conf.set(PRINCIPAL, props.getProperty("principal"));
            conf.set(KEYTAB, props.getProperty("keytab"));
            System.setProperty("java.security.krb5.conf", props.getProperty("krb5"));
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(conf.get(PRINCIPAL), conf.get(KEYTAB));
        }
    }

    /**
     * 初始化
     */
    private static void init() {
        try (InputStream propsFile = HdfsClient.class.getResource("hdfs-client,properties").openStream()) {
            props.load(new InputStreamReader(propsFile, StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error("hdfs-client init  error! ");
        }
        conf = new Configuration();
        conf.addResource(props.getProperty("hdfsSitePath"));
        conf.addResource(props.getProperty("coreSitePath"));
        //初始化hdfs文件系统
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            LOGGER.error("hdfs-fileSystem init  error! ");
        }
    }

    //读取Text文件，使用readHdfsFile
    public static JavaRDD<String> readTextFile(String path, int partitionCount) {
        JavaSparkContext sparkContext = sparkApp.contextBroadCast.value().get(0);
        JavaRDD<String> txtData =
                sparkContext.textFile(path, partitionCount).persist(StorageLevel.MEMORY_AND_DISK());
        return txtData;
    }

    //删除文件
    public static boolean deleteFile(String path) throws IOException {
        return fileSystem.delete(new Path(path), false);
    }

    //初始化hdfs文件系统
    public static void initHDFSFileSystem() throws URISyntaxException, IOException, InterruptedException {
        Configuration config = new Configuration();
        fileSystem = FileSystem.get(config);
    }

    //删除存在路径文件
    public static void deletefilepath(String filepath) throws IOException {
        String spath = filepath;
        Path path = new Path(spath);
        if (fileSystem.exists(path)) {
            FileStatus[] files = fileSystem.listStatus(path);
            for (FileStatus file : files) {
                fileSystem.delete(file.getPath(), false);
            }
            fileSystem.delete(path, true);
        }
        fileSystem.close();
    }

    //获取hdfs文件返回JavaRDD
    public static JavaPairRDD<LongWritable, Text> readHdfsNewApi(String path) {
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf, new Path(path));
        Configuration config = new Configuration();
        JavaPairRDD<LongWritable, Text> longWritableTextJavaPairRDD = sparkApp.contextBroadCast.value().get(0)
                .newAPIHadoopFile(path, CombineTextInputFormat.class, LongWritable.class, Text.class, config);

        return longWritableTextJavaPairRDD;
    }

    /**
     * 备用方法
     * @param path
     * @param partitions
     * @return
     */
    /*
    public static RDD<Tuple2<LongWritable,Text>> readHdfsRDD(String path,int partitions){
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf,new Path(path));
        RDD<Tuple2<LongWritable, Text>> tuple2RDD = sparkApp.sessionBroadcast.value().get(0)
                .sparkContext()
                .hadoopRDD(jobConf, TextInputFormat.class, LongWritable.class, Text.class, partitions);
        return tuple2RDD;
    }
    */
}
